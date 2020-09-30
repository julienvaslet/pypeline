from __future__ import annotations
from typing import Any, Callable, List, Type
import inspect


class Stage:
    next_identifier: int = 1

    def __init__(self, name: str):
        self.name = name
        self.order = Stage.next_identifier
        Stage.next_identifier += 1

    def __call__(self, function: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Callable:
            return function(*args, **kwargs)

        wrapper.__stage_name__ = self.name
        wrapper.__stage_order__ = self.order

        return wrapper


class StageImpl:
    __slots__ = [
        "name",
        "order",
        "pipeline",
        "function"
    ]

    def __init__(self, pipeline: PipelineImpl, function: Callable):
        self.name: str = function.__stage_name__
        self.order: int = function.__stage_order__
        self.pipeline: PipelineImpl = pipeline
        self.function: Callable = function

    def __eq__(self, other: StageImpl) -> bool:
        return self.order == other.order

    def __lt__(self, other: StageImpl) -> bool:
        return self.order < other.order

    def run(self, *args, **kwargs):
        print(f"[{self.name}] Running...")
        self.function(*args, **kwargs)
        print(f"[{self.name}] Executed.")


class Attribute:
    __slots__ = [
        "name",
        "type",
        "required",
        "description",
        "default"
    ]

    def __init__(self, name: str, a_type: Type, description: str = "", default: Any = None):
        self.name: str = name
        self.type: Type = a_type
        self.required: bool = False
        self.description: str = description

        if self.required and default is not None:
            raise SyntaxError("An attribute can't, at the same time, be required and have a default value.")

        self.default: Any = default


def Pipeline(_cls=None):
    def wrap(cls):
        class PipelineClass(PipelineImpl, cls):
            pass

        globals = {"PipelineClass": PipelineClass}

        # Note: __annotations__ is guaranted to be ordered
        cls_annotations = cls.__dict__.get("__annotations__", {})
        cls_attributes = []
        attributes = {}

        # TODO: Find attributes from parent classes definition

        # Find attributes from class definition
        for name, type_ in cls_annotations.items():
            # Ignore underscored/private variables
            if name.startswith("_"):
                continue

            default = getattr(cls, name, None)

            if isinstance(default, Attribute):
                attribute = default
            else:
                attribute = Attribute(name, type_, default=default)

            cls_attributes.append(attribute)

        for attribute in cls_attributes:
            attributes[attribute.name] = attribute

            # Replace manually defined attributes by their default values
            if isinstance(getattr(cls, attribute.name, None), Attribute):
                setattr(cls, attribute.name, attribute.default)

        # Check for misdefined attributes
        for name, value in cls.__dict__.items():
            if isinstance(value, Attribute) and name not in attributes:
                raise SyntaxError(f"{cls}.{name} has been defined but it's missing a type!")

        # Store the attributes in the __pipeline_attributes__ variable
        setattr(cls, "__pipeline_attributes__", attributes)

        # Create the class constructor
        # TODO: Check that there is no required arg after the ones with default values
        init_lines = ["super(PipelineClass, self).__init__()"]
        init_args = ["self"]

        for name, attribute in attributes.items():
            init_lines.append(f"self.{name}={name}")

            if attribute.required:
                init_args.append(f"{name}")
            else:
                globals[f"_default_{name}"] = attribute.default
                init_args.append(f"{name}=_default_{name}")

        locals = {}
        args = ", ".join(init_args)
        body = "\t" + "\n\t".join(init_lines)
        init_src = f"def __init__({args}) -> None:\n{body}"
        exec(init_src, globals, locals)

        PipelineClass.__init__ = locals["__init__"]

        return PipelineClass

    # Decorator called with parenthesis
    if _cls is None:
        return wrap

    # Decorator called without parenthesis
    return wrap(_cls)


class PipelineImpl:
    __slots__ = [
        "__stages__"
    ]

    def __init__(self):
        self.__stages__: List[StageImpl] = []

        # Initialize the stages
        def method_filter(m) -> bool:
            return inspect.ismethod(m) and "__stage_name__" in m.__dict__

        for _, method in inspect.getmembers(self, method_filter):
            self.__stages__.append(StageImpl(self, method))

        # Keep the original order
        self.__stages__.sort()

    def start(self) -> None:
        for stage in self.__stages__:
            stage.run()


@Pipeline
class MyPipeline:
    changelist: int
    value: str = "Default.txt"

    @Stage("Sync")
    def sync(self) -> None:
        print(f"Sync version {self.changelist}")

    @Stage("Build")
    def build(self) -> None:
        print(f"Build {self.value}")

    @Stage("Submit")
    def submit(self) -> None:
        print("Submit!")


if __name__ == "__main__":
    pipeline = MyPipeline(
        changelist=50,
        value="Hello.txt"
    )

    pipeline.start()

# TODO:
# - __status__: serializable state for hold, continue, remote tracking
# - thread execution: start(), join(), stop(), kill() etc.

# Read from https://github.com/ericvsmith/dataclasses/blob/master/dataclasses.py
