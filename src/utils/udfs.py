from utils import patterns
import re

def extract_framework_plattform(string: str):
    return [framework for framework in patterns.framework_plattforms if re.search(framework, string, re.IGNORECASE)]
def extract_language(string: str):
    return [language for language in patterns.languages if re.search(language.replace("+", "\+").replace("(", "\(").replace(")", "\)"), string, re.IGNORECASE)]

def extract_knowledge(string: str):
    return [knowledge for knowledge in patterns.knowledges if re.search(knowledge, string, re.IGNORECASE)]

def extract_design_pattern(string: str):
    return [design_pattern for design_pattern in patterns.design_patterns if re.search(design_pattern, string, re.IGNORECASE)]


def broadcast_labeled_knowledges(sc,labeled_knowledges):
    '''
    broadcast the mapped of labeled_knowledges to group data in knowledge field
    '''
    global mapped_knowledge
    mapped_knowledge = sc.broadcast(labeled_knowledges)

def labeling_knowledge(knowledge):
    try :
        return mapped_knowledge.value[knowledge]
    except :
        return None