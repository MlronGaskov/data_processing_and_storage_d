import random
import string


def gen_book_ref() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(6))


def gen_ticket_no() -> str:
    return "700" + "".join(random.choice(string.digits) for _ in range(10))
