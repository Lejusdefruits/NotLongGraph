import operator

from notlonggraph.channels import LastValue, BinaryOperatorAggregate


def test_channels() -> None:
    # ---- LastValue : écrase ----
    c = LastValue()
    c.update([42])                       # une seule écriture
    assert c.get() == 42, "LastValue devrait stocker 42"

    c.update([99])                       # nouvelle écriture -> écrase
    assert c.get() == 99, "LastValue devrait avoir écrasé par 99"

    c.update([])                         # writes vide -> rien ne change
    assert c.get() == 99, "writes vide ne doit rien changer"

    # ---- fresh_copy : un channel neuf et indépendant ----
    neuf = c.fresh_copy()
    assert isinstance(neuf, LastValue), "fresh_copy doit rendre un LastValue"
    neuf.update([7])
    assert neuf.get() == 7, "le channel neuf stocke 7"
    assert c.get() == 99, "l'original ne doit PAS être affecté par le neuf"

    # ---- BinaryOperatorAggregate : accumule avec '+' ----
    agg = BinaryOperatorAggregate(operator.add)
    agg.update([["a"]])                  # 1re écriture sert de base
    assert agg.get() == ["a"], "agg devrait valoir ['a']"

    agg.update([["b"], ["c"]])           # 2 écritures dans la même vague
    assert agg.get() == ["a", "b", "c"], "agg devrait accumuler -> ['a','b','c']"

    # ---- BinaryOp avec addition de nombres ----
    s = BinaryOperatorAggregate(operator.add)
    s.update([1, 2, 3])                  # 1 + 2 + 3
    assert s.get() == 6, "somme devrait valoir 6"

    # conflit LastValue (a activer quand la gestion sera choisie):
    # try:
    #     LastValue().update([1, 2])
    #     raise AssertionError("un conflit LastValue aurait du lever une erreur")
    # except Exception as e:
    #     print("conflit LastValue gere ->", type(e).__name__)


if __name__ == "__main__":
    test_channels()
    print("tous les tests passent")
