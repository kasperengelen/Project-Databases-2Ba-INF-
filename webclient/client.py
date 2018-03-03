from flask import Flask, flash, render_template, redirect, request, url_for, session
import user_utils
from db_wrapper import DBConnection
import utils

app = Flask(__name__)
app.config.update(dict(
    SECRET_KEY = "\xbf\xcf\xde\xee\xe8\xc1\x8c\\\xfd\xe6\\!t^(\x1c/\xc6l\xe1,\xc9#\xd7",
    WTF_CSRF_SECRET_KEY = "Uei\xc2&\x8a\x18.H\x87\xc5\x1d\xd1\xc8\xc3\xcf\xe5\xfft_\x8c:\x03r"
))
app.json_encoder = utils.CustomJSONEncoder

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login/', methods=['GET', 'POST'])
def login():
    """Callback for the login page."""
    return user_utils.login_user(request)

@app.route('/register/', methods=['GET', 'POST'])
def register():
    """Callback for the register page. This is used to create new users."""
    return user_utils.register_user(request)

@app.route('/logout/')
def logout():
    """Callback for the logout page. This simply overrides the session variables"""
    session["user_id"] = None
    session["logged_in"] = False

    flash(message="You are now logged out", category="success")
    return redirect(url_for("index"))

@app.route('/user/profile/')
def profile_self():
    """View the profile of the user who is logged in."""
    return user_utils.view_user(request, session['user_data'].user_id)

@app.route('/user/profile/<int:user_id>/')
def profile_other(user_id):
    """View the profile of other users."""
    return user_utils.view_user(request, user_id)

@app.route('/user/edit/')
def edit_userdata():
    """This page lets a user edit its own personal information."""
    return render_template('user_edit.html')

@app.route('/result/')
def result():
    """???"""
    render_template('result.html')

@app.route('/debug/')
def debug_page():
    """Debug page for testing stuff"""
    with DBConnection() as db_conn:
        db_conn.cursor().execute('SELECT * FROM user_accounts LIMIT 1');
        result = db_conn.cursor().fetchone()
        date = result[-1]

        print(date)
    return str(date)

#################################################### ERROR HANDLING PAGES ####################################################
@app.errorhandler(403)
def handle_403(e):
    return render_template('error.html', message="403, forbidden."), 403

@app.errorhandler(404)
def handle_404(e):
    return render_template('error.html', message="404, page not found."), 404

@app.errorhandler(500)
def handle_500(e):
    return render_template('error.html', message="500, internal error."), 500

if __name__ == '__main__':
    app.run(debug=True)
