from datetime import datetime
import time
import yaml

class ParseYaml:
    def __init__(self,filename) -> None:
        self.filename=filename
        self.parsedtext={}

    def load(self) -> dict:
        milestone = open(self.filename)
        self.parsedtext = yaml.load(milestone, Loader=yaml.FullLoader)
        return self.parsedtext


class Workflow:

    def __init__(self,dictionary,text) -> None:
        self.dictionary=dictionary
        self.flow=[]
        self.txt=text

    def iterate(self,name,description) -> None:
        self.flow.append({"name":name,"action":description})
        if description.get('Type') == "Flow" and description.get('Execution') == "Sequential":
            with open(self.txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Entry\n")
            for key,value in description['Activities'].items():
                self.iterate(name+"."+key,value)
            with open(self.txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Exit\n")


        elif description.get('Type') == "Task":
             TaskManager.ManageTask(name,description,self.txt)

    def calliterate(self) -> None:
        for key,value in self.dictionary.items():
            self.iterate(key,value)

    def checkflow(self) -> list:
        for value in self.flow:
            print("Name: ",value['name'])
            print("Action: ",value['action'])
        return self.flow


class TaskManager:

    @staticmethod
    def Timefunction(name,input,txt):
        with open(txt,"a") as log:
            log.write(str(datetime.now())+";"+name+" Entry\n")
            log.write(str(datetime.now())+";"+name+" Executing TimeFunction({},{})\n".format(input.get("FunctionInput"),input.get("ExecutionTime")))
        time.sleep(int(input.get('ExecutionTime')))
        with open(txt,"a") as log:
            log.write(str(datetime.now())+";"+name+" Exit\n")

    @staticmethod
    def ManageTask(name,description,txt):
         print(name,description,txt)
         if description.get('Function') == "TimeFunction":
             TaskManager.Timefunction(name,description.get('Inputs'),txt)

if __name__ == "__main__":
    milestone1A = ParseYaml('./Milestone1/Milestone1A.yaml')
    parsed_yaml_fileA = milestone1A.load()

    yaml1=Workflow(parsed_yaml_fileA,'Milestone1A.txt')
    yaml1.calliterate()

    yaml1.checkflow()

    milestone1B = ParseYaml('./Milestone1/Milestone1B.yaml')
    parsed_yaml_fileB = milestone1B.load()

    yaml2=Workflow(parsed_yaml_fileB,'Milestone1B.txt')
    yaml2.calliterate()

    print(parsed_yaml_fileB)