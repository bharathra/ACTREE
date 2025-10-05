#! /usr/bin/env python3

from collections import deque
from typing import Dict, Any, Optional, List

import pendulum
import networkx as nx
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from action import Action


class Agent:

    def _is_goal_met(self, current_state: Dict[str, Any], goal: Dict[str, Any]) -> bool:
        """Checks if the current state satisfies the goal state."""
        return all(current_state.get(k) == v for k, v in goal.items())

    def _update_system_state(self, current_state: Dict[str, Any], action: Action) -> Dict[str, Any]:
        """Applies the effects of an action to the current state."""
        new_state = current_state.copy()
        new_state.update(action.effects)
        return new_state

    def _get_hashable_state(self, state: Dict[str, Any]) -> tuple:
        """Converts a state dict to a hashable tuple for the visited set."""
        # Only include keys from the goal in the hash to keep the state space small
        # and focused (local state principle).
        return tuple(sorted((k, v) for k, v in state.items()))

    # The main planner function
    def plan(self, actions: Dict[str, Action], initial_state: Dict[str, Any], goal: Dict[str, Any]) -> Optional[nx.DiGraph]:
        """
        Finds the shortest action sequence using BFS and converts it to a parallel DAG.
        
        Args:
            actions: Dictionary of all available actions (ActionManager.actions).
            initial_state: The starting state of the local problem.
            goal: The desired goal state.
        
        Returns:
            A NetworkX DiGraph representing the parallel plan, or None if no plan is found.
        """

        # BFS Initialization
        # Queue stores (current_state, path_of_actions)
        queue: deque = deque([(initial_state, [])])
        # Visited set tracks states to prevent cycles and redundant work
        visited: set = {self._get_hashable_state(initial_state)}

        final_linear_plan: Optional[List[Action]] = None

        # State-Space Search (BFS)
        while queue:
            current_state, current_path = queue.popleft()

            # 1. Goal Check
            if self._is_goal_met(current_state, goal):
                final_linear_plan = current_path
                break  # BFS guarantees the shortest path, so we stop

            # 2. Try All Available Actions
            for action_name, action in actions.items():

                # Check if action's preconditions are met
                if all(current_state.get(k) == v for k, v in action.preconditions.items()):

                    new_state = self._update_system_state(current_state, action)
                    new_state_hash = self._get_hashable_state(new_state)

                    if new_state_hash not in visited:
                        visited.add(new_state_hash)
                        new_path = current_path + [action]
                        queue.append((new_state, new_path))

        if not final_linear_plan:
            return None  # No plan found

        # 3. Convert Linear Plan to Parallel DAG (Dependency Analysis)
        return self._build_dependency_graph(final_linear_plan)

    def _build_dependency_graph(self, plan: List[Action]) -> nx.DiGraph:
        """
        Converts the linear sequence into a DAG by finding dependencies.
        (This logic is based on our previous discussion.)
        """
        G = nx.DiGraph()

        for action in plan:
            # Use the action name as the node ID
            G.add_node(action.name, action_obj=action)

        for i, current_action in enumerate(plan):
            # Check for dependencies with all preceding actions
            for j in range(i):
                preceding_action = plan[j]

                # A dependency exists if a precondition of the current action
                # is satisfied by an effect of a preceding action.
                for pre_key, pre_val in current_action.preconditions.items():
                    if pre_key in preceding_action.effects:
                        # If a key is in the effects, it means the state was changed,
                        # thus establishing a dependency.
                        G.add_edge(preceding_action.name, current_action.name)
                        break  # Only need one dependency to link the actions

        return G


    def create_dynamic_dag(self, graph_plan, dag_id_suffix):
        """
        Creates an Airflow DAG from a NetworkX graph.
        """
        dag_id = f'action_graph_plan_{dag_id_suffix}'

        with DAG(
            dag_id=dag_id,
            start_date=pendulum.datetime(2025, 9, 6, tz="UTC"),
            schedule=None,
            catchup=False,
            tags=['action_graph', 'dynamic'],
            default_args={'owner': 'airflow'}
        ) as dag:
            # Create a dictionary to hold the Airflow tasks
            tasks = {}

            # 1. Create a PythonOperator for each node (action) in the graph
            for node_name in graph_plan.nodes:
                tasks[node_name] = PythonOperator(
                    task_id=node_name,
                    python_callable=self.execute_action_script,
                    op_kwargs={'action_name': node_name}
                )

            # 2. Set the dependencies based on the graph's edges
            for u, v in graph_plan.edges:
                tasks[v].set_upstream(tasks[u])

        return dag

    # This is the function that will be called by the PythonOperator.
    # It simulates the execution of an action's script.
    def execute_action_script(self, action_name, **kwargs):
        """A callable function to run the action's script."""
        print(f"Executing script for action: {action_name}")
        # You would retrieve the script from a database or file
        # and execute it here. For now, we'll use a simple lambda
        # stored in the node's attributes.
        dag_run_conf = kwargs['dag_run'].conf
        action_script = dag_run_conf['graph'].nodes[action_name]['script']
        action_script()
