# bulk-task

Package bulk-task provide an easy way to call lazy functions in bulk.

## Installation

```
$ pip install bulk-task
```

## Example

```python
from dataclasses import dataclass
from bulk_task import BulkTask


bulk_task = BulkTask()


# It can be a dataclass or a Pydantic model.
@dataclass
class DataclassModel:
    name: str


@bulk_task
def func(args: List[DataclassModel]):
    print(arg.name for arg in args)


# Push lazy calls.
func.push('name1')
func.push('name2')

# Bulk consume lazy calls.
bulk_task.consume()
```
