from typing import Type
from ast import AST
from synthesis import Synthesizer


def transpile(ast: Type[AST], target: Type[Synthesizer]):
    return target(ast).generate()
