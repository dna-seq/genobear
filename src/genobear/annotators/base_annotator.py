from typing import Dict, TypeVar, Generic, Optional, Any

# --- Generic Base Annotator ---

# Define TypeVars to allow the Annotator to be generic over input and output data types.
InputType = TypeVar('InputType')
OutputType = TypeVar('OutputType')

class Annotator(Generic[InputType, OutputType]):
    """
    A generic, self-contained annotator.

    Each annotator is a callable object that, when called, first ensures all its
    dependencies are processed and then applies its own annotation logic.

    This design removes the need for a central manager.
    """
    def __init__(self, name: str, *dependencies: 'Annotator[Any, InputType]'):
        """
        Initializes the annotator with its name and dependency objects.

        Args:
            name: The unique name for this annotator.
            *dependencies: A variable number of other Annotator instances that
                         this one depends on.
        """
        if not name:
            raise ValueError("Annotator name cannot be empty.")
        self._name = name
        self._dependencies = dependencies

    @property
    def name(self) -> str:
        """Returns the unique name of the annotator."""
        return self._name

    @property
    def dependencies(self) -> tuple['Annotator[Any, InputType]', ...]:
        """Returns the tuple of dependency annotator instances."""
        return self._dependencies

    def annotate(self, data: InputType, **kwargs: Any) -> OutputType:
        """
        The specific annotation logic for this annotator. Implementations may
        transform the input type to a different output type.

        Subclasses must implement this method. This method should only contain
        the logic for the current annotator and can assume that all dependencies
        have already been processed.

        Args:
            data: The input data to be annotated.
            **kwargs: Additional data or parameters required for annotation.

        Returns:
            The annotated data, possibly with a different type than the input.
        """
        raise NotImplementedError("Each annotator must implement the 'annotate' method.")

    def __call__(self, data: InputType, context: Optional[Dict[str, Any]] = None, **kwargs: Any) -> OutputType:
        """
        Executes the annotation pipeline for this annotator and its dependencies.

        This method orchestrates the run by:
        1. Recursively calling its dependencies.
        2. Using a shared context to ensure each annotator in the chain is
           processed only once per run.
        3. Calling its own `annotate` method.

        Args:
            data: The initial input data for the pipeline.
            context: A dictionary used internally to track processed annotators.
            **kwargs: Additional parameters to be passed to all annotators.

        Returns:
            The final, annotated data.
        """
        # Initialize the context for the first call in a pipeline.
        # The context is passed through the entire recursive call stack.
        if context is None:
            context = {'processed': set(), 'results': {}}

        # 1. Process all dependencies first. Each dependency must produce the
        #    input type required by this annotator.
        prepared_data: InputType = data
        for dep in self._dependencies:
            prepared_data = dep(prepared_data, context=context, **kwargs)

        # 2. After dependencies are met, run this annotator's specific logic,
        #    but only if it hasn't already been run in this pipeline context.
        if self.name not in context['processed']:
            print(f"-> Running annotator: '{self.name}'")
            result: OutputType = self.annotate(prepared_data, **kwargs)
            context['processed'].add(self.name)
            # cache the result for potential repeated calls in the same context
            context['results'][self.name] = result
            return result

        # Already processed: return the cached OutputType result
        cached_result = context.get('results', {}).get(self.name)
        if cached_result is not None:
            return cached_result  # type: ignore[return-value]
        # Fallback (should not occur): run annotate to produce an OutputType
        return self.annotate(prepared_data, **kwargs)

    def __repr__(self) -> str:
        dep_names = ", ".join([d.name for d in self._dependencies])
        return f"{self.__class__.__name__}(name='{self.name}', deps=[{dep_names}])"

