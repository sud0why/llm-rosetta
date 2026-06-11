"""Argo OpenAI Chat schema transforms.

Request-side (to_transforms)
-----------------------------
``rename_field("max_tokens", "max_completion_tokens")`` converts the deprecated
``max_tokens`` parameter to ``max_completion_tokens`` for newer OpenAI models
(GPT-4o, o1, etc.) that reject the old name.
"""

from llm_rosetta.shims.transforms import rename_field

to_transforms = (rename_field("max_tokens", "max_completion_tokens"),)
from_transforms = ()
