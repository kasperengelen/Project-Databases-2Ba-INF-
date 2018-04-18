from flask import session
from UserManager import UserManager

class LoginManager:
    """Class that manages the session login-related variables."""
    @staticmethod
    def setLoggedIn(user):
        """Set the specified user as the logged-in user."""
        session['loggedin'] = True
        session['userdata'] = user.toDict()
    # ENDMETHOD

    @staticmethod
    def setLoggedOut():
        """Logout the currently logged in user. If no user is
        logged in, nothing happens."""
        session['loggedin'] = False
        session['userdata'] = None
    # ENDMETHOD

    @staticmethod
    def syncSession():
        """Update the information about the logged-in user stored 
        in the session variables with the information stored in the 
        database."""

        userid = session['userdata']['userid']
        session['userdata'] = UserManager.getUserFromID(userid).toDict()
    # ENDMETHOD
# ENDCLASS
