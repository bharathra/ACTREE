import pendulum
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
import networkx as nx

# This is a placeholder for your ActionGraph planner.
# In a real setup, this would be a separate module you import.


class ActionGraphPlanner:
    def plan(self, initial_state, goal):
        # This method would return a NetworkX DiGraph
        # For demonstration, we'll return a simple graph
        G = nx.DiGraph()
        G.add_node("drive_to_store", script=lambda: print("Driving to store..."))
        G.add_node("get_groceries", script=lambda: print("Getting groceries..."))
        G.add_node("cook_dinner", script=lambda: print("Cooking dinner..."))

        G.add_edge("drive_to_store", "get_groceries")
        G.add_edge("get_groceries", "cook_dinner")

        return G

# This is the function that will be called by the PythonOperator.
# It simulates the execution of an action's script.


def execute_action_script(action_name, **kwargs):
    """A callable function to run the action's script."""
    print(f"Executing script for action: {action_name}")
    # You would retrieve the script from a database or file
    # and execute it here. For now, we'll use a simple lambda
    # stored in the node's attributes.
    dag_run_conf = kwargs['dag_run'].conf
    action_script = dag_run_conf['graph'].nodes[action_name]['script']
    action_script()


def create_dynamic_dag(graph_plan, dag_id_suffix):
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
                python_callable=execute_action_script,
                op_kwargs={'action_name': node_name},
            )

        # 2. Set the dependencies based on the graph's edges
        for u, v in graph_plan.edges:
            tasks[u] >> tasks[v]

    return dag


# Assuming your planner is ready to run
my_planner = ActionGraphPlanner()
# Let's get a graph for a plan named "dinner_plan_1"
my_graph = my_planner.plan(initial_state={}, goal={})

# Generate and register the DAG with Airflow
globals()['action_graph_dag'] = create_dynamic_dag(my_graph, 'dinner_plan_1')
