#! /usr/bin/env python3

class Action:
    def __init__(self, name: str, preconditions: dict, effects: dict, script: str):
        self.name = name
        self.preconditions = preconditions
        self.effects = effects
        self.script = script

    def __repr__(self) -> str:
        return f"Action(name='{self.name}', preconditions={self.preconditions}, effects={self.effects})"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "preconditions": self.preconditions,
            "effects": self.effects,
            "script": self.script
        }

    @staticmethod
    def from_dict(data: dict) -> 'Action':
        return Action(
            name=data["name"],
            preconditions=data["preconditions"],
            effects=data["effects"],
            script=data["script"]
        )
