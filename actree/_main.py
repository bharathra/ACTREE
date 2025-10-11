#! /usr/bin/env python3

from action_lib import ActionLib
from agent import Agent


# Example Usage
if __name__ == "__main__":
    money = 11000
    # Initialize
    axn_map = ActionLib("actions.json")
    # sample actions
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

    # plan
    my_graph = agent.plan(axn_map.actions, initial_state, goal)
    print(my_graph)
    # execute
    agent.execute_linearly(my_graph, initial_state)
