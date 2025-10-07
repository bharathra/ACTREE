#! /usr/bin/env python3

import operator
from collections import deque
from typing import Dict, Any, Optional, List

import pendulum
import networkx as nx
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from action import Action

# Map string operators to Python's operator functions
OP_MAP = {
    '==': operator.eq,
    '!=': operator.ne,
    '>': operator.gt,
    '<': operator.lt,
    '>=': operator.ge,
    '<=': operator.le,
    # For binary states (True/False):
    'is': operator.eq,
}


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

    def _check_preconditions(self, current_state: Dict[str, Any], action: 'Action') -> bool:
        """
        Checks if the current state satisfies the action's preconditions, including relations.

        Assumed Action Precondition format: {'state_key': ('operator', value)}
        Example: {'fuel': ('>=', 5)}
        """
        for state_key, required_condition in action.preconditions.items():
            current_value = current_state.get(state_key)

            # Check for binary states (no relation, just key=value)
            if not isinstance(required_condition, tuple):
                if current_value != required_condition:
                    return False
                continue

            # Check for relational states
            op_str, required_value = required_condition
            op_func = OP_MAP.get(op_str)

            if op_func and current_value is not None:
                if not op_func(current_value, required_value):
                    return False
            elif current_value is None:
                # Cannot check condition if the state key does not exist
                return False

        return True

    def _simulate_success(self, current_state: Dict[str, Any], action: 'Action', goal: Dict[str, Any]) -> Dict[str, Any]:
        """
        IMPLEMENTATION OF ASSUME SUCCESS: 
        Simulates the action achieving the part of the goal it affects.

        Assumed Action Effect format: {'goal_key': 'ASSUME_ACHIEVED'}
        """
        new_state = current_state.copy()

        for effect_key, effect_value in action.effects.items():
            # If the action's effect is a variable/goal-directed, we assume it achieves the goal target.
            if effect_value == 'ASSUME_ACHIEVED':
                # The new state's value for this key becomes the goal's value.
                if effect_key in goal:
                    new_state[effect_key] = goal[effect_key]
                # If the action has a non-variable effect, apply it normally
            else:
                new_state[effect_key] = effect_value

        return new_state

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

                # Use relational check
                if self._check_preconditions(current_state, action):

                    # Assume Success State Transition
                    new_state = self._simulate_success(current_state, action, goal)
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

    def create_dynamic_dag(self, graph_plan: nx.DiGraph, dag_id_suffix: str) -> DAG:
        """
        Creates an Airflow DAG from a NetworkX graph, including a failure/replan task.
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
            tasks = {}

            # 1. Create a PythonOperator for each planned action
            for node_name in graph_plan.nodes:
                tasks[node_name] = PythonOperator(
                    task_id=node_name,
                    python_callable=self.execute_action_script,
                    op_kwargs={'action_name': node_name},
                )

            # 2. Set the dependencies based on the graph's edges
            for u, v in graph_plan.edges:
                tasks[v].set_upstream(tasks[u])

            # 3. Define the Re-Planner Contingency Task (New Logic)

            # This task is called to initiate the next planning cycle if anything fails.
            replan_task = PythonOperator(
                task_id='REPLAN_FLOW_TRIGGER',
                python_callable=self.trigger_replan_loop,
                # THIS IS THE CRITICAL LINE: It runs only if ONE task fails.
                trigger_rule=TriggerRule.ONE_FAILED,
            )

            # 4. Connect ALL actions to the replan_task
            # This ensures that if any task fails, the replan_task is triggered.
            # We must use set_downstream to connect all tasks created so far.
            all_action_tasks = list(tasks.values())

            if all_action_tasks:
                # Sets the replan_task downstream of every other task in the DAG
                replan_task.set_upstream(all_action_tasks)

        return dag

    # --- New Callback Function for the REPLAN_FLOW_TRIGGER task ---
    def trigger_replan_loop(self, **kwargs):
        """
        This function is executed by Airflow when an action fails.
        It should call your external trigger system to kick off a new planning loop.
        """
        ti = kwargs['ti']
        dag_run_id = kwargs['dag_run'].run_id

        # In a real system, this function would:
        # 1. Find the failed task using ti.get_direct_upstream_failed_task_instances()
        # 2. Extract the current system state (via XComs or a database).
        # 3. Call your external agent/API to start the planning process again with the new state.

        print(f"!!! REPLANNING TRIGGERED !!!")
        print(f"DAG Run {dag_run_id} failed. Calling agent loop to re-plan...")

        # We would place the Airflow API trigger call here to start a NEW, smaller DAG.
        # For simplicity, we'll just print.
        # trigger_external_agent_api(failed_task_info, current_state)
        pass

    # This is the function that will be called by the PythonOperator.
    def execute_action_script_test(self, action_script: str, current_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the action script in a defined scope, allowing it to access and modify the state.

        Args:
            action_script: The Python code string to execute.
            current_state: The dictionary representing the system state.

        Returns:
            The modified system state dictionary.
        """
        # print(f"[TEST RUN] Executing script: {action_script.strip()}")
        # 1. Define the local scope for the script
        # We pass the state in as a local variable named 'STATE'
        # The script must use 'STATE' to access and modify the system state.
        local_scope = {'STATE': current_state}
        # 2. Execute the script
        # We pass an empty dictionary for globals and our local_scope for locals.
        exec(action_script, {}, local_scope)
        # 3. Return the modified state dictionary
        return local_scope['STATE']

    # Assuming a dedicated key for the entire system state, e.g., 'system_state'
    def execute_action_script(self, action_name: str, **context) -> Dict[str, Any]:
        """
        Pulls the state, executes the action, and pushes the modified state.
        """
        ti = context['ti']
        # 1. PULL STATE: Retrieve the most recent state pushed by an upstream task
        # Note: XComs are usually retrieved from a specific upstream task,
        # but for simplicity, we assume the latest 'system_state' is correct.
        current_state: Dict[str, Any] = ti.xcom_pull(task_ids=None, key='system_state')

        if not current_state:
            # Load initial state if this is the first task
            current_state = context['dag_run'].conf.get('initial_state', {})
            print("[SETUP] Loaded initial state from DAG config.")

        # 2. EXECUTE AND OBSERVE: (Your LLM-generated script logic runs here)
        # This is where your script would read current_state['fuel'] and observe
        # the non-deterministic outcome, e.g., using a custom function.

        # --- SIMULATION OF EXECUTION AND VARIABLE EFFECT ---
        # Assume the action execution returns the observed changes
        observed_effects = {"fuel": current_state.get('fuel', 0) + 2, "item_count": 1}
        # ----------------------------------------------------

        # 3. APPLY EFFECTS AND PUSH NEW STATE
        current_state.update(observed_effects)

        # The return value of a Python callable is automatically pushed to XCom
        # with the key 'return_value'. We will explicitly push the whole state
        # with a standard key for downstream tasks to find easily.
        ti.xcom_push(key='system_state', value=current_state)

        print(f"[OBSERVED] State updated. New fuel level: {current_state['fuel']}")
        return current_state  # Also push as return_value for convenience

    def execute_dag_locally(self, dag_graph: nx.DiGraph, current_state: Dict[str, Any] = {}):
        """
        Executes the DAG in a dependency-respecting, serial order.
        """
        # 1. Get the serial execution order
        try:
            execution_order = list(nx.topological_sort(dag_graph))
        except nx.NetworkXUnfeasible:
            # This means your graph has a cycle (e.g., A depends on B, B depends on A)
            print("❌ ERROR: The plan contains a cyclic dependency and cannot be executed.")
            return

        print("✅ Valid execution order found. Running tasks sequentially:")

        # 2. Execute each action
        for action_name in execution_order:
            # Retrieve the original Action object (assuming you store it in the node attributes)
            action_obj = dag_graph.nodes[action_name].get('action_obj')

            if action_obj:
                # print(f"  [RUN] Executing: {action_name}")
                # Call your validation and execution function here:
                self.execute_action_script_test(action_obj.script, current_state)
            else:
                print(f"  [WARN] Action object not found for {action_name}")
