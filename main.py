from ParseYaml import *
from datetime import datetime


class TaskManager:

    def __init__(self) -> None:
        self.date_time = datetime.fromtimestamp(1887639468)

    def Timefunction(self,Exetime) -> None:
        print(Exetime)


if __name__ == "__main__":
    milestone1A = ParseYaml('./Milestone1/Milestone1A.yaml')
    parsed_yaml_fileA = milestone1A.load()


    milestone1B = ParseYaml('./Milestone1/Milestone1B.yaml')
    parsed_yaml_fileB = milestone1B.load()

    print(parsed_yaml_fileB)