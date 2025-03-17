"""iso check for container numbers"""

import re
# import polars as pl


class ContainerValidator:
    """Validates a container number"""

    def __init__(self) -> None:
        pass

    def read_input(self, data: list[str] = None):
        """Reads the container numbers"""
        if data:
            container_numbers = data
            return [number.strip() for number in container_numbers]
        container_numbers = input(
            "Enter container numbers separated by commas: ").split(',')
        return [number.strip() for number in container_numbers]


    def validate_container_number(self,container_number:list[str]):
        """validates the container numbers"""

        pattern = re.compile(r"[A-Z]{4}[0-9]{6}[0-9]$")

        try:
            if len(container_number) != 11:
                raise ValueError(
                    f"Container number '{container_number} is not 11 characters long."
                )
            if not pattern.match(container_number):
                raise ValueError(
                    f"Container number '{container_number}' does not match the required format."
                )
            if not self.validate_check_digit(container_number):
                raise ValueError(
                    f"Container number '{container_number}' has an invalid check digit."
                )
            return True

        except ValueError as e:
            print(e)
            return False

    def validate_check_digit(self, container_number) -> bool:
        """Calculates the check digit"""
        value_map: dict[str, int] = {
            "A": 10,
            "B": 12,
            "C": 13,
            "D": 14,
            "E": 15,
            "F": 16,
            "G": 17,
            "H": 18,
            "I": 19,
            "J": 20,
            "K": 21,
            "L": 23,
            "M": 24,
            "N": 25,
            "O": 26,
            "P": 27,
            "Q": 28,
            "R": 29,
            "S": 30,
            "T": 31,
            "U": 32,
            "V": 34,
            "W": 35,
            "X": 36,
            "Y": 37,
            "Z": 38,
        }

        total_sum = 0
        for i in range(10):
            if i < 4:
                total_sum += value_map[container_number[i]] * (2**i)
            else:
                total_sum += int(container_number[i]) * (2**i)

        check_digit = total_sum % 11
        check_digit %= 10
        return check_digit == int(container_number[10])

    def validate_container_numbers(self,container_numbers):
        """Validate the container numbers"""

        results = {}
        for number in container_numbers:
            results[number] = self.validate_container_number(number)
        for cnumber, is_valid in results.items():
            if not is_valid:
                print(f"Container Number: '{cnumber}' is invalid")
            else:
                print("all okay!")
