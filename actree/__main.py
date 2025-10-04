#! /usr/bin/env python3

from actions import ActionManager


# Example Usage
if __name__ == "__main__":
    # Initialize the manager
    manager = ActionManager("actions.json")
    # Create some sample actions
    drive_to_store_script = "print('Driving to the store...')"
    manager.create_action("drive_to_store", {"car_fueled": True}, {"location": "store", "car_fueled": False}, drive_to_store_script)
    go_home_script = "print('Going home...')"
    manager.create_action("go_home", {"location": "store"}, {"location": "home"}, go_home_script)
    buy_car_script = "print('Buying a new car...')"
    manager.create_action("buy_car", {"has_money": True}, {"has_car": True}, buy_car_script)
    # Save the actions
    manager.save_actions()

    # Now, let's get actions that result in being at the 'store'
    store_actions = manager.get_actions_by_effect("location", "store")
    print(f"\nActions that result in being at the store: {store_actions}")
    # Let's get actions that result in having a 'car'
    has_car_actions = manager.get_actions_by_effect("has_car", True)
    print(f"\nActions that result in having a car: {has_car_actions}")
