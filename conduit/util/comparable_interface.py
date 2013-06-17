"""
This class is an unmodified copy of one from a blog post written by Lennart Regebro at:
http://regebro.wordpress.com/2010/12/13/python-implementing-rich-comparison-the-correct-way/
No license terms were specified in the blog post.

It has been included as a minor convenience.
If we ever need to purge our code of external contributions, replacement should be fairly trivial.
"""


class Comparable(object):
    def _compare(self, other, method):
        try:
            return method(self._cmpkey(), other._cmpkey())
        except (AttributeError, TypeError):
            # _cmpkey not implemented, or return different type,
            # so I can't compare with "other".
            return NotImplemented

    def __lt__(self, other):
        return self._compare(other, lambda s,o: s < o)

    def __le__(self, other):
        return self._compare(other, lambda s,o: s <= o)

    def __eq__(self, other):
       return self._compare(other, lambda s,o: s == o)

    def __ge__(self, other):
        return self._compare(other, lambda s,o: s >= o)

    def __gt__(self, other):
        return self._compare(other, lambda s,o: s > o)

    def __ne__(self, other):
        return self._compare(other, lambda s,o: s != o)
