"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""


class FeedExists(Exception):
    pass

class FeedDoesNotExist(Exception):
    pass

class Empty(Exception):
    pass

class NotListening(Exception):
    pass