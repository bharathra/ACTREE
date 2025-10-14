#! /usr/bin/env python3

from typing import Any, Dict, List

import pendulum
import networkx as nx

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from action import Action


class AirFlowInterface:
    """Interface to create and manage Airflow DAGs based on action plans.
    """

    def _build_dependency_graph(self,
                                plan: List[Action]) -> nx.DiGraph:
        """
        Converts the linear sequence into a DAG by finding dependencies.
        """
        dg = nx.DiGraph()

        for action in plan:
            # Use the action name as the node ID
            dg.add_node(action.name, action_obj=action)

        for i, current_action in enumerate(plan):
            # Check for dependencies with all preceding actions
            for j in range(i):
                preceding_action = plan[j]

                # A dependency exists if a precondition of the current action
                # is satisfied by an effect of a preceding action.
                for pre_key, _ in current_action.preconditions.items():
                    if pre_key in preceding_action.effects:
                        # If a key is in the effects, it means the state was changed,
                        # thus establishing a dependency.
                        dg.add_edge(preceding_action.name, current_action.name)
                        break  # Only need one dependency to link the actions

        return dg

    def create_dynamic_dag(self,
                           final_linear_plan: List[Action],
                           dag_id_suffix: str) -> DAG:
        """
        Creates an Airflow DAG from a NetworkX graph, including a failure/replan task.
        """

        # Convert Linear Plan to Parallel DAG (Dependency Analysis)
        graph_plan = self._build_dependency_graph(final_linear_plan)

        with DAG(dag_id=f"action_graph_plan_{dag_id_suffix}",
                 start_date=pendulum.datetime(2025, 9, 6, tz="UTC"),
                 schedule=None,
                 catchup=False,
                 tags=["action_graph", "dynamic"],
                 default_args={"owner": "airflow"}
                 ) as dag:

            tasks = {}

            # 1. Create a PythonOperator for each planned action
            for node_name in graph_plan.nodes:
                tasks[node_name] = PythonOperator(
                    task_id=node_name,
                    python_callable=self.execute_action_script,
                    op_kwargs={"action_name": node_name},
                )

            # 2. Set the dependencies based on the graph's edges
            for u, v in graph_plan.edges:
                tasks[v].set_upstream(tasks[u])

            # 3. Define the Re-Planner Contingency Task (New Logic)

            # This task is called to initiate the next planning cycle if anything fails.
            replan_task = PythonOperator(
                task_id="REPLAN_FLOW_TRIGGER",
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

    # --- Callback Function for the REPLAN_FLOW_TRIGGER task ---
    def trigger_replan_loop(self, **kwargs):
        """
        This function is executed by Airflow when an action fails.
        It should call the external trigger system to kick off a new planning loop.
        """
        # ti = kwargs["ti"]
        dag_run_id = kwargs["dag_run"].run_id

        # In a real system, this function would:
        # 1. Find the failed task using ti.get_direct_upstream_failed_task_instances()
        # 2. Extract the current system state (via XComs or a database).
        # 3. Call your external agent/API to start the planning process again with the new state.

        print("!!! REPLANNING TRIGGERED !!!")
        print(f"DAG Run {dag_run_id} failed. Calling agent loop to re-plan...")

        # We would place the Airflow API trigger call here to start a NEW, smaller DAG.
        # For simplicity, we'll just print.
        # trigger_external_agent_api(failed_task_info, current_state)

    # Assuming a dedicated key for the entire system state, e.g., 'system_state'
    def execute_action_script(self, **context) -> Dict[str, Any]:
        """
        Pulls the state, executes the action, and pushes the modified state.
        """
        ti = context["ti"]
        # 1. PULL STATE: Retrieve the most recent state pushed by an upstream task
        # Note: XComs are usually retrieved from a specific upstream task,
        # but for simplicity, we assume the latest 'system_state' is correct.
        current_state: Dict[str, Any] = ti.xcom_pull(task_ids=None, 
                                                     key="system_state")

        if not current_state:
            # Load initial state if this is the first task
            current_state = context["dag_run"].conf.get("initial_state", {})
            print("[SETUP] Loaded initial state from DAG config.")

        # 2. EXECUTE AND OBSERVE: (Your LLM-generated script logic runs here)
        # This is where your script would read current_state['fuel'] and observe
        # the non-deterministic outcome, e.g., using a custom function.

        # --- SIMULATION OF EXECUTION AND VARIABLE EFFECT ---
        # Assume the action execution returns the observed changes
        observed_effects = {"fuel": current_state.get("fuel", 0) + 2,
                            "item_count": 1}
        # ----------------------------------------------------

        # 3. APPLY EFFECTS AND PUSH NEW STATE
        current_state.update(observed_effects)

        # The return value of a Python callable is automatically pushed to XCom
        # with the key 'return_value'. We will explicitly push the whole state
        # with a standard key for downstream tasks to find easily.
        ti.xcom_push(key="system_state", value=current_state)

        print(f"[OBSERVED] State updated. New fuel level: {current_state['fuel']}")
        return current_state  # Also push as return_value for convenience
