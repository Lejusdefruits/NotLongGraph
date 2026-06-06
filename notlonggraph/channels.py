# Channels: regle de fusion d'une cle du state (reducers).
# A ecrire: Channel (base), LastValue, BinaryOperatorAggregate.
# Test: python -m notlonggraph.channels  ou  pytest tests/test_channels.py


# --- classes ici ---


if __name__ == "__main__":
    from tests.test_channels import test_channels

    test_channels()
    print("tous les tests passent")
