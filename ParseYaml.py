import yaml

class ParseYaml:
    def __init__(self,filename) -> None:
        self.filename=filename
        self.parsedtext={}

    def load(self) -> dict:
        milestone = open(self.filename)
        self.parsedtext = yaml.load(milestone, Loader=yaml.FullLoader)
        return self.parsedtext
