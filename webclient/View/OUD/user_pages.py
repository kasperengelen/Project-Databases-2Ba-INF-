from flask import render_template, request, abort, Blueprint, flash, redirect, url_for, session
from utils import require_login
from utils import LoginManager
from user_forms import UserLoginForm, UserRegisterForm
from passlib.hash import sha256_crypt
from UserManager import UserManager
from user_forms import UserEditForm

user_pages = Blueprint('user_pages', __name__)

@user_pages.route('/logout/', methods=['GET'])
@require_login
def logout():
    """Returns a page that when accessed logs out the user."""
    LoginManager.setLoggedOut()

    flash(message="You are now logged out.", category="success")
    return redirect(url_for('index'))

@user_pages.route('/login/', methods=['GET', 'POST'])
def login():
    """Returns a page that provides a way to login a user."""
    
    form = UserLoginForm(request.form)

    if request.method == 'POST' and form.validate():
        if UserManager.validateLogin(form.email.data, form.password.data):
            user = UserManager.getUserFromEmail(form.email.data)
            LoginManager.setLoggedIn(user)
            flash(message="You are now logged in.", category="success")
            return redirect(url_for('index'))
        else:
            flash(message="Invalid email and password combination.", category="error")
            return render_template('login.html', form=form)
    else:
        return render_template('login.html', form = form)
# ENDFUNCTION

@user_pages.route('/register/', methods=['GET', 'POST'])
def register():
    """Returns a page that provides a way to create a new user."""
    
    form = UserRegisterForm(request.form)

    if request.method == 'POST' and form.validate():
        if UserManager.existsEmail(form.email.data):
            flash(message="Specified e-mail address is already in use.", category="error")
            return render_template('register.html', form=form)
        else:
            fname = form.firstname.data
            lname = form.lastname.data
            email = form.email.data
            password = form.password.data

            UserManager.createUser(email, password, fname, lname, False)

            flash(message="You are now registered as a user.", category="success")

            return redirect(url_for('user_pages.login'))
    else:
        return render_template('register.html', form=form)
# ENDFUNCTION

@user_pages.route('/user/profile/', defaults={'user_id': None})
@user_pages.route('/user/profile/<int:user_id>/')
@require_login
def profile(user_id):
    """Returns a page that contains information about a user."""
    
    if user_id is None:
        user_id = session['userdata']['userid']

    if not UserManager.existsID(user_id):
        abort(404)

    user_data = UserManager.getUserFromID(user_id)

    return render_template('user_profile.html', user_data = user_data.toDict())
# ENDFUNCTION

@user_pages.route('/user/edit/', methods=['GET', 'POST'])
@require_login
def edit():
    """Returns a page that provides a way to edit user information."""

    userid = session['userdata']['userid']

    current_user = UserManager.getUserFromID(userid)

    # CHECK IF USER EXISTS
    if not UserManager.existsID(userid):
        abort(404)

    form = UserEditForm(request.form)

    if request.method == 'POST' and form.validate():

        # check if new email is in use.
        if form.email.data != current_user.email and UserManager.existsEmail(form.email.data):
            flash(message="Specified e-mail address already in use.", category="error")
            return render_template('user_edit.html', form = form)

        # update information
        new_email = form.email.data
        new_fname = form.firstname.data
        new_lname = form.lastname.data
        new_pass = form.password.data

        current_user.editInfo(new_email, new_fname, new_lname, new_pass)
        session['userdata'] = current_user.toDict()
        flash(message="Information updated.", category="success")
    else:
        form.fillFields(session['userdata'])
    
    return render_template('user_edit.html', form = form)
# ENDFUNCTION

@user_pages.route('/user/delete/', methods = ['POST'])
@require_login
def delete():
    """Callback to delete the user."""

    # DELETE USER
    UserManager.destroyUser(session['userdata']['userid'])

    # LOGOUT
    LoginManager.setLoggedOut()

    flash(message="User account deleted.", category="success")

    return redirect(url_for('index'))
# ENDFUNCTION