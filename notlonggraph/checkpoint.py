"""Time-travel : snapshot global de l'etat a chaque super-step.

Un checkpoint = une photo FIGEE de tous les channels a la frontiere d'un
super-step, plus de quoi reprendre (le numero de step et la vague suivante).
La copie est faite en copy-on-write : on ne deepcopy que les channels qui ont
change pendant la vague, on partage la reference des autres.
"""
import copy
from dataclasses import dataclass

from notlonggraph.engine import read_state


@dataclass
class Checkpoint:
    step: int
    channels: dict   # channels GELES (issus du copy-on-write)
    active: list      # nodes a executer a la vague suivante


def make_checkpoint(live, changed_keys, frozen_prev):
    """Copy-on-write : deepcopy uniquement les channels modifies, partage le reste.

    Un channel est copie si :
      - il a ete ecrit cette vague (`key in changed_keys`),
      - ou il n'existait pas au checkpoint precedent (`key not in frozen_prev`),
      - ou son type mute en silence a on_step_end (TTL, ephemere...).
    Sinon on reutilise la reference figee du checkpoint precedent.
    """
    frozen = {}
    for key, ch in live.items():
        if key in changed_keys or key not in frozen_prev or ch.mutates_on_step_end:
            frozen[key] = copy.deepcopy(ch)
        else:
            frozen[key] = frozen_prev[key]
    return frozen


class MemoryCheckpointer:
    """Garde l'historique des checkpoints en RAM.

    C'est une implementation parmi d'autres : un SqliteCheckpointer pourrait
    exposer la meme interface (save / get_state_history) sans toucher l'engine.
    """
    def __init__(self):
        self.history: list[Checkpoint] = []

    def save(self, step, live_channels, changed_keys, active):
        prev = self.history[-1].channels if self.history else {}
        frozen = make_checkpoint(live_channels, changed_keys, prev)
        self.history.append(Checkpoint(step, frozen, list(active)))

    def get_state_history(self):
        """Les etats lisibles (vue .get) de chaque super-step, dans l'ordre."""
        return [read_state(cp.channels) for cp in self.history]
