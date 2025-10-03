#! /usr/bin/env python3

import json
from typing import Dict, Optional, List
from action import Action


class ActionManager:
    def __init__(self, storage_file: str):
        self.storage_file = storage_file
        self.actions: Dict[str, Action] = {}
        self.effect_index: Dict[str, List[str]] = {}
        self.load_actions()
        self._build_effect_index()

    def _build_effect_index(self):
        """Builds the inverted index mapping effects to action names."""
        self.effect_index.clear()
        for action_name, action in self.actions.items():
            for effect_key, effect_value in action.effects.items():
                effect_tuple = (effect_key, effect_value)
                effect_str = str(effect_tuple)
                if effect_str not in self.effect_index:
                    self.effect_index[effect_str] = []
                self.effect_index[effect_str].append(action_name)

    def create_action(self, name: str, preconditions: dict, effects: dict, script: str) -> Action:
        """Creates a new Action and adds it to the library, then updates the index."""
        if name in self.actions:
            pass

        new_action = Action(name, preconditions, effects, script)
        self.actions[name] = new_action
        self._build_effect_index()  # Rebuild the index after a new action is added
        return new_action

    def get_action(self, name: str) -> Optional[Action]:
        """Retrieves an action by its name."""
        return self.actions.get(name)

    def delete_action(self, name: str) -> bool:
        """Deletes an action from the library and updates the index."""
        if name in self.actions:
            del self.actions[name]
            self._build_effect_index()  # Rebuild the index after an action is deleted
            return True
        return False

    def save_actions(self):
        """Saves the current actions to the storage file."""
        with open(self.storage_file, 'w') as f:
            action_dicts = [action.to_dict() for action in self.actions.values()]
            json.dump(action_dicts, f, indent=4)
        print(f"Successfully saved {len(self.actions)} actions to {self.storage_file}")

    def load_actions(self):
        """Loads actions from the storage file."""
        try:
            with open(self.storage_file, 'r') as f:
                action_dicts = json.load(f)
                self.actions = {
                    action_data["name"]: Action.from_dict(action_data)
                    for action_data in action_dicts
                }
            print(f"Successfully loaded {len(self.actions)} actions from {self.storage_file}")
        except (FileNotFoundError, json.JSONDecodeError):
            print("No existing actions file found or file is empty. Starting with an empty library.")
            self.actions = {}

    def get_actions_by_effect(self, effect_key: str, effect_value: any) -> List[Action]:
        """
        Retrieves a list of actions that have a given effect.
        """
        effect_str = str((effect_key, effect_value))
        action_names = self.effect_index.get(effect_str, [])
        return [self.actions[name] for name in action_names]


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
