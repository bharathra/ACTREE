#! /usr/bin/env python3


class Action:
    """
    Represents an action that can be performed by the agent.
    """

    def __init__(self, 
                 name: str,
                 preconditions: dict,
                 effects: dict,
                 script: str):
        """
        Initializes a new Action.

        Args:
            name (str): The name of the action.
            preconditions (dict): A dictionary of conditions that must be met
                before the action can be executed.
            effects (dict): A dictionary of changes to the state of the world
                that will result from executing the action.
            script (str): A Python script that will be executed when the action
                is performed.
        """

        self.name = name
        self.preconditions = preconditions
        self.effects = effects
        self.script = script

    def __repr__(self) -> str:
        return f"{self.name}"

    def to_dict(self) -> dict:
        """Serializes the Action to a dictionary."""
        return {
            "name": self.name,
            "preconditions": self.preconditions,
            "effects": self.effects,
            "script": self.script
        }

    @staticmethod
    def from_dict(data: dict) -> 'Action':
        """Deserializes an Action from a dictionary."""
        return Action(
            name=data["name"],
            preconditions=data["preconditions"],
            effects=data["effects"],
            script=data["script"]
        )
