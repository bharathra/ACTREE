#! /usr/bin/env python3

from action_lib import ActionLib
from agent import Agent


# Example Usage
if __name__ == "__main__":

    # Initialize the manager
    axn_map = ActionLib("actions.json")
    # Create some sample actions
    axn_map.create_action("eat_food",
                          {"hungry": True, "location": "store"},
                          {"hungry": False},
                          "print('Eating food...')")

    axn_map.create_action("drive_to_store", 
                          {"car_fueled": True}, 
                          {"location": "store"}, 
                          "print('Driving to the store...')")

    axn_map.create_action("drive_to_home",
                          {"car_fueled": True},
                          {"location": "home"},
                          "print('Driving back to home...')")
    
    axn_map.create_action("fill_gas", 
                          {"has_car": True},
                          {"car_fueled": True},
                          "print('Going home...')")
    
    axn_map.create_action("buy_car", 
                          {"has_money": True}, 
                          {"has_car": True}, 
                          "print('Buying a new car...')")
    # Save the actions
    axn_map.save_actions()

    # Getting actions by desired effect
    # store_actions = axn_map.get_actions_by_effect("location", "store")
    # print(f"\nActions that result in requested effect: {store_actions}")

    # Let's get a graph for a plan named "dinner_plan_1"
    # Generate and register the DAG with Airflow
    agent = Agent()
    my_graph = agent.plan(actions=axn_map.actions, initial_state={
                          "has_money": True, 
                          "hungry": True, 
                          "location": "home", 
                          "car_fueled": False, 
                          "has_car": False},
                          goal={"hungry": False, "location": "home"})
    globals()['action_graph_dag'] = agent.create_dynamic_dag(my_graph, 'dinner_plan_1')

    # print graph
    if my_graph is not None:
        print(my_graph.nodes)

