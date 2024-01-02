"""Module containing functions used to interact with the RDS database."""

def get_user_by_id(user_id: int) -> dict:
    """Returns user in User table with given user_id; return None if no match found."""