

class WalkerEntry():
    def __init__(self, fi, good=True, ex=None, done=False):
        self.fi = fi
        self.good = good
        self.ex = ex
        self.done = done

    def __str__(self):
        return f"FindEntry(path={self.fi.abspath()}, done={self.done}, ex={self.ex})"

    def __repr__(self):
        return self.__str__()

