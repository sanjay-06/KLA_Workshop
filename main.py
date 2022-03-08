from datetime import datetime
import time
import yaml
import threading
import pandas as pd

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
        self.lock=threading.Lock()

    def iterate(self,name,description) -> None:
        self.flow.append({"name":name,"action":description})
        if description.get('Type') == "Flow" and description.get('Execution') == "Sequential":
            with open(self.txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Entry\n")
            for key,value in description['Activities'].items():
                self.iterate(name+"."+key,value)
            with open(self.txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Exit\n")

        elif description.get('Type') == "Flow" and description.get('Execution') == "Concurrent":
            threadlist=[]
            with open(self.txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Entry\n")
            for key,value in description['Activities'].items():
                threadlist.append(threading.Thread(target=self.iterate,args=(name+"."+key,value,)))

            for thread in threadlist:
                thread.start()

            for thread in threadlist:
                thread.join()

            with open(self.txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Exit\n")


        elif description.get('Type') == "Task":
             Task =TaskManager()
             Task.ManageTask(name,description,self.txt,self.lock)

    def calliterate(self) -> None:
        for key,value in self.dictionary.items():
            self.iterate(key,value)

        # print(self.flow)

    def checkflow(self) -> list:
        print(self.flow)

defecttrack={}
class TaskManager:

    def __init__(self) -> None:
        self.lock=threading.Lock()

    def Timefunction(self,name,description,txt,lock) -> None:
        print((description.get('Inputs')).get('FunctionInput'))
        with open(txt,"a") as log:
            log.write(str(datetime.now())+";"+name+" Entry\n")
        time.sleep(int((description.get('Inputs')).get("ExecutionTime")))
        print(description.get("Condition"))
        if(description.get("Condition")):
            task,oper,value=description.get("Condition").split(" ")
            if oper == ">":
                if not defecttrack[task] > int(value):
                    with open(txt,"a") as log:
                        log.write(str(datetime.datetime.now())+";"+name+" Entry\n"+str(datetime.datetime.now())+";"+name+" skipped\n"+str(datetime.datetime.now())+";"+name+" Exit\n")
                else:
                    with open(txt,"a") as log:
                        log.write(str(datetime.now())+";"+name+" Executing TimeFunction({},{})\n".format(((description.get('Inputs')).get('FunctionInput')),((description.get('Inputs')).get('FunctionInput'))))

            elif oper == "<":
                if not defecttrack[task] < int(value):
                    with open(txt,"a") as log:
                        log.write(str(datetime.datetime.now())+";"+name+" Entry\n"+str(datetime.datetime.now())+";"+name+" skipped\n"+str(datetime.datetime.now())+";"+name+" Exit\n")
                else:
                    with open(txt,"a") as log:
                        log.write(str(datetime.now())+";"+name+" Executing TimeFunction({},{})\n".format(((description.get('Inputs')).get('FunctionInput')),((description.get('Inputs')).get('FunctionInput'))))
            self.lock.acquire()
            defecttrack[f"$({name+'.NoOfDefects'})"]=(description.get("Outputs"))[1]
            self.lock.release()
        else:
             with open(txt,"a") as log:
                log.write(str(datetime.now())+";"+name+" Executing TimeFunction({},{})\n".format(((description.get('Inputs')).get('FunctionInput')),((description.get('Inputs')).get('FunctionInput'))))


        with open(txt,"a") as log:
            log.write(str(datetime.now())+";"+name+" Exit\n")

    def Dataload(self,name,description,txt,lock) -> None:

        with open(txt,"a") as log:
            log.write(str(datetime.now())+";"+name+" Entry\n")

        (description.get("Outputs"))[0]=pd.read_csv("./Milestone2/"+(description.get("Inputs")).get("Filename"))
        (description.get("Outputs"))[1]=len((description.get("Outputs"))[0])
        print(description.get("Condition"))
        if(description.get("Condition")):
            task,oper,value=description.get("Condition").split(" ")
            if oper == ">":
                if not defecttrack[task] > int(value):
                    with open(txt,"a") as log:
                        log.write(str(datetime.now())+";"+name+" Entry\n"+str(datetime.datetime.now())+";"+name+" skipped\n"+str(datetime.datetime.now())+";"+name+" Exit\n")
                else:
                    with open(txt,"a") as log:
                        log.write(str(datetime.now())+";"+name+" Executing DataLoad ({})\n".format((description.get('Inputs')).get('Filename')))
            elif oper == "<":
                if not defecttrack[task] < int(value):
                    with open(txt,"a") as log:
                        log.write(str(datetime.now())+";"+name+" Entry\n"+str(datetime.datetime.now())+";"+name+" skipped\n"+str(datetime.datetime.now())+";"+name+" Exit\n")
                else:
                    with open(txt,"a") as log:
                        log.write(str(datetime.now())+";"+name+" Executing DataLoad ({})\n".format((description.get('Inputs')).get('Filename')))
            self.lock.acquire()
            defecttrack[f"$({name+'.NoOfDefects'})"]=(description.get("Outputs"))[1]
            self.lock.release()
        else:
             with open(txt,"a") as log:
                 log.write(str(datetime.now())+";"+name+" Executing DataLoad ({})\n".format((description.get('Inputs')).get('Filename')))


        with open(txt,"a") as log:
            log.write(str(datetime.now())+";"+name+" Exit\n")

    def ManageTask(self,name,description,txt,lock) -> None:

         print(name,description,txt)
         if description.get('Function') == "TimeFunction":
             self.Timefunction(name,description,txt,lock)
         if description.get('Function') == "DataLoad":
             self.Dataload(name,description,txt,lock)

if __name__ == "__main__":
    milestone1A = ParseYaml('./Milestone2/Milestone2A.yaml')
    parsed_yaml_fileA = milestone1A.load()

    yaml1=Workflow(parsed_yaml_fileA,'Milestone2A.txt')
    yaml1.calliterate()

    print(defecttrack)
    # yaml1.checkflow()

    defecttrack={}

    milestone1B = ParseYaml('./Milestone2/Milestone2B.yaml')
    parsed_yaml_fileB = milestone1B.load()

    yaml2=Workflow(parsed_yaml_fileB,'Milestone2B.txt')
    yaml2.calliterate()

    # print(parsed_yaml_fileB)