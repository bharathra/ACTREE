#! /usr/bin/env python3

from action_lib import ActionLib
from agent import Agent


# Example Usage
if __name__ == "__main__":
    money = 20000
    # Initialize the manager
    axn_map = ActionLib("actions.json")
    # Create some sample actions
    axn_map.create_action("eat_food",
                          {"hungry": True, "location": "store", "has_money": money >= 5},
                          {"hungry": False},
                          "STATE['money'] -= 5\n" \
                          "mon=STATE['money']\n" \
                          "print(f'Eating food; Available money: {mon}')")

    axn_map.create_action("drive_to_store",
                          {"car_fueled": True},
                          {"location": "store"},
                          "print(f'Driving to the store...')")

    axn_map.create_action("drive_to_home",
                          {"car_fueled": True},
                          {"location": "home"},
                          "print(f'Driving back to home...')")

    axn_map.create_action("fill_gas",
                          {"has_car": True, "has_money": money >= 100},
                          {"car_fueled": True},
                          "STATE['money'] -= 100\n" \
                          "mon=STATE['money']\n" \
                          "print(f'Filling gas; Available money: {mon}')")

    axn_map.create_action("buy_car",
                          {"has_money": money >= 10000},
                          {"has_car": True},
                          "STATE['money']-= 10000\n" \
                          "mon=STATE['money']\n" \
                          "print(f'Buying a new car; Available money: {mon}')")
    # Save the actions
    axn_map.save_actions()

    # Getting actions by desired effect
    # store_actions = axn_map.get_actions_by_effect("location", "store")
    # print(f"\nActions that result in requested effect: {store_actions}")

    agent = Agent()
    initial_state = {
        "has_money": money > 0,
        "hungry": True,
        "location": "home",
        "car_fueled": False,
        "has_car": False,
        "money": money
    }
    goal = {
        "hungry": False,
        "location": "home"
    }

    # Let's get a graph for a plan
    my_graph = agent.plan(axn_map.actions, initial_state, goal)

    # print graph
    if my_graph is None:
        print("No plan found.")
    else:
        print(f"Dependencies: {my_graph.edges}")
        print(f"Plan: {my_graph.nodes}")

    # execute
    if my_graph is not None:
        agent.execute_dag_locally(my_graph, initial_state)

    # Generate and register the DAG with Airflow
    # dag = agent.create_dynamic_dag(my_graph, 'dinner_plan_1')
