# Metaflow Deep Dive (3) - Decorators

Previous Post(s):
> [Metaflow Deep Dive (2) - Runtime](./metaflow-deep-dive-2.md)
>
> [Metaflow Deep Dive (1) - Static Analysis](./metaflow-deep-dive-1.md)
> 
> [Metaflow Deep Dive (0) - Overview](./metaflow-deep-dive-0.md)

## Decorators in Metaflow

### Base Decorators

```python
# metaflow/decorators.py

def _base_step_decorator(decotype, *args, **kwargs):
    """
    Decorator prototype for all step decorators. This function gets specialized
    and imported for all decorators types by _import_plugin_decorators().
    """
    if args:
        # No keyword arguments specified for the decorator, e.g. @foobar.
        # The first argument is the function to be decorated.
        func = args[0]
        if not hasattr(func, "is_step"):
            raise BadStepDecoratorException(decotype.name, func)

        # Only the first decorator applies
        if decotype.name in [deco.name for deco in func.decorators]:
            raise DuplicateStepDecoratorException(decotype.name, func)
        else:
            func.decorators.append(decotype(attributes=kwargs, statically_defined=True))

        return func
    else:
        # Keyword arguments specified, e.g. @foobar(a=1, b=2).
        # Return a decorator function that will get the actual
        # function to be decorated as the first argument.
        def wrap(f):
            return _base_step_decorator(decotype, f, **kwargs)

        return wrap
```