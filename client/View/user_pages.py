from flask import render_template, request, abort, Blueprint, flash, redirect, url_for, session
from Controller.AccessController import require_login
from Controller.LoginManager import LoginManager
from Controller.UserManager import UserManager
from View.user_forms import UserEditInfoForm, UserLoginForm, UserRegisterForm, UserEditPasswordForm
from View.form_utils import flash_errors

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

            if not user.active:
                flash(message="Error: User is deactivated.", category="error")
                return render_template('user_pages.login.html', form=form)

            LoginManager.setLoggedIn(user)
            flash(message="You are now logged in.", category="success")
            return redirect(url_for('index'))
        else:
            flash(message="Invalid email and password combination.", category="error")
            return render_template('user_pages.login.html', form=form)
    else:
        # FORM ERRORS ARE DISPLAYED UNDERNEATH FIELDS
        return render_template('user_pages.login.html', form = form)
# ENDFUNCTION

@user_pages.route('/register/', methods=['GET', 'POST'])
def register():
    """Returns a page that provides a way to create a new user."""
    
    form = UserRegisterForm(request.form)

    if request.method == 'POST' and form.validate():
        if UserManager.existsEmail(form.email.data):
            flash(message="Specified e-mail address is already in use.", category="error")
            return render_template('user_pages.register.html', form=form)
        else:
            fname = form.firstname.data
            lname = form.lastname.data
            email = form.email.data
            password = form.password.data

            UserManager.createUser(email, password, fname, lname, False)

            flash(message="You are now registered as a user.", category="success")

            return redirect(url_for('user_pages.login'))
    else:
        # FORM ERRORS ARE DISPLAYED UNDERNEATH FIELDS
        return render_template('user_pages.register.html', form=form)
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

    userinfo = UserManager.getUserFromID(user_id)
    editform = UserEditInfoForm()
    editform.fillForm(userinfo)

    return render_template('user_pages.profile.html', 
                                userinfo = userinfo.toDict(), 
                                editform = editform,
                                editpassform = UserEditPasswordForm())
# ENDFUNCTION

@user_pages.route('/user/edit_info/', methods=['POST'])
@require_login
def edit_info():
    """Callback to edit user info."""

    userid = session['userdata']['userid']

    # CHECK IF USER EXISTS
    if not UserManager.existsID(userid):
        return redirect(url_for('user_pages.profile'))

    form = UserEditInfoForm(request.form)

    if not form.validate():
        flash_errors(form)
    else:
        current_user = UserManager.getUserFromID(userid)

        # check if new email is in use.
        if form.email.data != current_user.email and UserManager.existsEmail(form.email.data):
            flash(message="Specified e-mail address already in use.", category="error")
            return render_template('user_pages.edit.html', form = form)

        # update information
        new_email = form.email.data
        new_fname = form.firstname.data
        new_lname = form.lastname.data

        UserManager.editUserInfo(userid, new_fname, new_lname, new_email)

        LoginManager.syncSession()
        flash(message="Information updated.", category="success")
    
    return redirect(url_for('user_pages.profile'))
# ENDFUNCTION

@user_pages.route('/user/edit_pass/', methods=['POST'])
@require_login
def edit_pass():
    """Callback to edit password."""
    
    userid = session['userdata']['userid']

    # CHECK IF USER EXISTS
    if not UserManager.existsID(userid):
        return redirect(url_for('user_pages.profile'))

    form = UserEditPasswordForm(request.form)

    if not form.validate():
        flash(message="Invalid form.", category="error")
        flash_errors(form)
    else:
        current_user = UserManager.getUserFromID(userid)

        new_pass = form.password.data

        UserManager.editUserPass(userid, new_pass)
        flash(message="Password updated.", category="success")

    return redirect(url_for('user_pages.profile'))
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
