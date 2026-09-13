"""Typed label affinities for sandbox placement."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Tuple


_KINDS = {"resource": 0, "instance": 1}
_AFFINITIES = {
    "preferred": 0,
    "preferred_anti": 1,
    "required": 2,
    "required_anti": 3,
}
_OPERATORS = {"in": 0, "not_in": 1, "exists": 2, "not_exists": 3}


@dataclass(frozen=True)
class LabelOperator:
    """Match a label key using membership or key existence.

    ``in`` matches any supplied value; ``not_in`` also matches missing keys.
    Existence operators take no values. Values are copied to an immutable tuple.
    """

    key: str
    operator: Literal["in", "not_in", "exists", "not_exists"] = "in"
    values: Sequence[str] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.key, str) or not self.key.strip():
            raise ValueError("label key must be a non-empty string")
        if not isinstance(self.operator, str) or self.operator not in _OPERATORS:
            raise ValueError("operator must be in, not_in, exists, or not_exists")
        if isinstance(self.values, (str, bytes)) or not isinstance(self.values, Sequence):
            raise TypeError("label values must be a sequence of strings")
        values = tuple(self.values)
        if not all(isinstance(value, str) for value in values):
            raise TypeError("label values must contain only strings")
        if self.operator in ("in", "not_in") and not values:
            raise ValueError("in and not_in require label values")
        if self.operator in ("exists", "not_exists") and values:
            raise ValueError("exists and not_exists do not accept label values")
        object.__setattr__(self, "values", values)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": _OPERATORS[self.operator],
            "labelKey": self.key,
            "labelValues": list(self.values),
        }


@dataclass(frozen=True)
class ScheduleAffinity:
    """One group of label expressions using the scheduler's affinity semantics.

    Expressions in a group are ANDed. Required groups are also ANDed unless
    ``preferred_priority`` selects ordered alternatives. Preferred groups guide
    scoring; ``preferred_anti_other_labels`` plus priority makes resource
    preferences required. Groups sharing a selector must agree on priority.
    """

    label_ops: Sequence[LabelOperator]
    kind: Literal["resource", "instance"] = "resource"
    affinity: Literal[
        "preferred", "preferred_anti", "required", "required_anti"
    ] = "required"
    preferred_priority: bool = False
    preferred_anti_other_labels: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or self.kind not in _KINDS:
            raise ValueError("kind must be resource or instance")
        if not isinstance(self.affinity, str) or self.affinity not in _AFFINITIES:
            raise ValueError(
                "affinity must be preferred, preferred_anti, required, or required_anti"
            )
        for name in ("preferred_priority", "preferred_anti_other_labels"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a boolean")
        if isinstance(self.label_ops, (str, bytes)) or not isinstance(
            self.label_ops, Sequence
        ):
            raise TypeError("label_ops must be a sequence of LabelOperator objects")
        operators = tuple(self.label_ops)
        if not operators:
            raise ValueError("label_ops must not be empty")
        if not all(isinstance(op, LabelOperator) for op in operators):
            raise TypeError("label_ops must contain only LabelOperator objects")
        object.__setattr__(self, "label_ops", operators)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": _KINDS[self.kind],
            "affinity": _AFFINITIES[self.affinity],
            "labelOps": [op.to_dict() for op in self.label_ops],
            "preferredPriority": self.preferred_priority,
            "preferredAntiOtherLabels": self.preferred_anti_other_labels,
        }

    def _selector(self) -> Tuple[str, str]:
        affinity = self.affinity
        if (
            self.kind == "resource"
            and self.preferred_priority
            and self.preferred_anti_other_labels
        ):
            affinity = {
                "preferred": "required", "preferred_anti": "required_anti"
            }.get(affinity, affinity)
        return self.kind, affinity


def _build_schedule_affinities(
    affinities: Optional[Sequence[ScheduleAffinity]],
    runtime: Optional[str],
    node_id: Optional[str],
) -> list[Dict[str, Any]]:
    if affinities is None:
        affinities = ()
    if isinstance(affinities, (str, bytes)) or not isinstance(affinities, Sequence):
        raise TypeError("schedule_affinities must be a sequence of ScheduleAffinity objects")
    constraints = []
    if runtime is not None:
        constraints.append(LabelOperator("sandbox.runtime", values=(runtime,)))
    if node_id is not None:
        constraints.append(LabelOperator("NODE_ID", values=(node_id,)))
    priorities = {}
    result = []
    has_required_resource = False
    for item in affinities:
        if not isinstance(item, ScheduleAffinity):
            raise TypeError("schedule_affinities must contain only ScheduleAffinity objects")
        selector = item._selector()
        if selector in priorities and priorities[selector] != item.preferred_priority:
            raise ValueError("affinities sharing a selector must agree on preferred_priority")
        priorities[selector] = item.preferred_priority
        encoded = item.to_dict()
        if selector == ("resource", "required"):
            # Constrain every ordered alternative. A standalone required group
            # would become an OR branch under priority and bypass caller labels.
            encoded["labelOps"].extend(op.to_dict() for op in constraints)
            has_required_resource = True
        result.append(encoded)
    if constraints and not has_required_resource:
        result.append(ScheduleAffinity(tuple(constraints)).to_dict())
    return result
