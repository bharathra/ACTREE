#! /usr/bin/env python3

import operator
from collections import deque
from typing import Dict, Any, List

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

    def _is_goal_met(self,
                     current_state: Dict[str, Any],
                     goal: Dict[str, Any]) -> bool:
        """Checks if the current state satisfies the goal state."""
        return all(current_state.get(k) == v for k, v in goal.items())

    def _update_system_state(self,
                             current_state: Dict[str, Any],
                             action: Action) -> Dict[str, Any]:
        """Applies the effects of an action to the current state."""
        new_state = current_state.copy()
        new_state.update(action.effects)
        return new_state

    def _get_hashable_state(self, state: Dict[str, Any]) -> tuple:
        """Converts a state dict to a hashable tuple for the visited set."""
        # Only include keys from the goal in the hash to keep the state space small
        # and focused (local state principle).
        return tuple(sorted((k, v) for k, v in state.items()))

    def _check_preconditions(self,
                             current_state: Dict[str, Any],
                             action: 'Action') -> bool:
        """
        Checks if the current state satisfies the action's preconditions, including relations.
        Assumed Action Precondition format: {'state_key': ('operator', value)}
        Example: {'fuel': ('>=', 5)}
        """
        for state_key, required_condition in action.preconditions.items():
            current_value = current_state.get(state_key)
            #
            # Check for binary states (no relation, just key=value)
            if not isinstance(required_condition, tuple):
                if current_value != required_condition:
                    return False
                continue
            #
            # Check for relational states
            op_str, required_value = required_condition
            op_func = OP_MAP.get(op_str)
            if op_func and current_value is not None:
                if not op_func(current_value, required_value):
                    return False
            elif current_value is None:
                # Cannot check condition if the state key does not exist
                return False
            #
        return True

    def _simulate_success(self, current_state: Dict[str, Any], action: 'Action', goal: Dict[str, Any]) -> Dict[str, Any]:
        """
        IMPLEMENTATION OF ASSUME SUCCESS: 
        Simulates the action achieving the part of the goal it affects.
        Assumed Action Effect format: {'goal_key': 'ASSUME_ACHIEVED'}
        """
        new_state = current_state.copy()
        #
        for effect_key, effect_value in action.effects.items():
            # If the action's effect is a variable/goal-directed, we assume it achieves the goal target.
            if effect_value == 'ASSUME_ACHIEVED':
                # The new state's value for this key becomes the goal's value.
                if effect_key in goal:
                    new_state[effect_key] = goal[effect_key]
                # If the action has a non-variable effect, apply it normally
            else:
                new_state[effect_key] = effect_value
            #
        return new_state

    # The main planner function
    def plan(self, actions: Dict[str, Action],
             initial_state: Dict[str, Any],
             goal: Dict[str, Any]) -> List[Action]:
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
        final_linear_plan: List[Action] = []
        #
        # State-Space Search (BFS)
        while queue:
            current_state, current_path = queue.popleft()
            # 1. Goal Check
            if self._is_goal_met(current_state, goal):
                final_linear_plan = current_path
                break  # BFS guarantees the shortest path, so we stop
                #
            # 2. Try All Available Actions
            for action_name, action in actions.items():
                # Use relational check
                if self._check_preconditions(current_state, action):
                    # Assume Success State Transition
                    new_state = self._simulate_success(current_state, action, goal)
                    new_state_hash = self._get_hashable_state(new_state)
                    #
                    if new_state_hash not in visited:
                        visited.add(new_state_hash)
                        new_path = current_path + [action]
                        queue.append((new_state, new_path))
            #
        return final_linear_plan

    def execute_action_script(self, action_script: str, current_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the action script in a defined scope, allowing it to access and modify the state.
        Args:
            action_script: The Python code string to execute.
            current_state: The dictionary representing the system state.
        Returns:
            The modified system state dictionary.
        """
        # 1. Define the local scope for the script
        # Pass the state in as a local variable named 'STATE'
        # The script must use 'STATE' to access and modify the system state.
        local_scope = {'STATE': current_state}
        # 2. Execute the script
        # Pass an empty dictionary for globals and our local_scope for locals.
        exec(action_script, {}, local_scope)
        # 3. Return the modified state dictionary
        return local_scope['STATE']

    def execute_linearly(self, actions: List[Action], current_state: Dict[str, Any] = {}):
        if not actions:
            print("No actions to execute.")
            return
            #
        # Execute actions
        print("Valid execution order found. Running tasks sequentially:")
        for action in actions:
            if not action:
                continue
            # print(f"  [RUN] Executing: {action_name}")
            self.execute_action_script(action.script, current_state)
