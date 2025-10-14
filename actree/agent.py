#! /usr/bin/env python3

import operator
from collections import deque
from typing import Dict, Any, List, Optional

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
    """
    The Agent class is responsible for planning and executing actions.
    It uses a simple forward-chaining planner to find a sequence of actions
    that can achieve a given goal state from an initial state.
    """

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

    def _get_hashable_state(self,
                            state: Dict[str, Any]) -> tuple:
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
        for state_key, required_value in action.preconditions.items():
            current_value = current_state.get(state_key)
            # print(f"Checking precondition for '{state_key}': {required_value}")
            #
            # Check for binary states (no relation, just key=value)
            if not isinstance(required_value, tuple):
                if current_value != required_value:
                    return False
                continue
                #
            # Check for relational states
            op_str, required_value = required_value
            # If the values start with '#', then they are refering to another state key
            if isinstance(required_value, str) and \
                    required_value[0] == '#':
                required_value = current_state.get(required_value[1:])
                #
            op_func = OP_MAP.get(op_str)
            if op_func and current_value is not None:
                if not op_func(current_value, required_value):
                    return False
            elif current_value is None:
                # Cannot check condition if the state key does not exist
                return False
                #
        return True

    def _simulate_success(self,
                          current_state: Dict[str, Any],
                          action: Action) -> Dict[str, Any]:
        """
        Simulates the action achieving the part of the goal it affects.
        """
        new_state = current_state.copy()
        #
        for effect_key, effect_value in action.effects.items():
            new_state[effect_key] = effect_value
            #
        return new_state

    # The main planner function
    def plan(self,
             actions: Dict[str, Action],
             initial_state: Dict[str, Any],
             goal: Dict[str, Any]) -> List[Action]:
        """
        Finds the shortest action sequence using BFS and 
        converts it to a parallel DAG.
        Args:
            actions: Dictionary of all available actions (ActionManager.actions).
            initial_state: The starting state of the local problem.
            goal: The desired goal state.
        Returns:
            A NetworkX DiGraph representing the parallel plan, 
            or None if no plan is found.
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
            for action in actions.values():
                # Use relational check
                if self._check_preconditions(current_state, action):
                    # Assume Success State Transition
                    new_state = self._simulate_success(current_state, action)
                    new_state_hash = self._get_hashable_state(new_state)
                    #
                    if new_state_hash not in visited:
                        visited.add(new_state_hash)
                        new_path = current_path + [action]
                        queue.append((new_state, new_path))
            #
        return final_linear_plan

    def execute_linearly(self,
                         actions: List[Action],
                         current_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Executes a list of actions sequentially.
        Args:
            actions: A list of actions to execute.
            current_state: The initial state of the system.
        """
        if not actions:
            print("No actions to execute.")
            return
            #
        if current_state is None:
            current_state = {}
            #
        # Execute actions
        print("Valid execution order found. Running tasks sequentially:")
        for action in actions:
            if not action:
                continue
            # 1. Define the local scope for the script
            # Pass the state in as a local variable named 'STATE'
            # The script must use 'STATE' to access and modify the system state.
            local_scope = {'STATE': current_state}
            # 2. Execute the script
            # Pass an empty dictionary for globals and our local_scope for locals.
            # print(f"  [RUN] Executing: {action_name}")
            exec(action.script, {}, local_scope)
