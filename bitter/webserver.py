import json
import os.path
from flask import Flask, redirect, session, url_for, request, flash, render_template
from twitter import Twitter
from twitter import OAuth as TOAuth

from flask_oauthlib.client import OAuth

from . import utils
from . import config

oauth = OAuth()
twitter = oauth.remote_app('twitter',
            base_url='https://api.twitter.com/1/',
            request_token_url='https://api.twitter.com/oauth/request_token',
            access_token_url='https://api.twitter.com/oauth/access_token',
            authorize_url='https://api.twitter.com/oauth/authenticate',
            consumer_key=config.CONSUMER_KEY,
            consumer_secret=config.CONSUMER_SECRET
            )

app = Flask(__name__)

@twitter.tokengetter
def get_twitter_token(token=None):
    return session.get('twitter_token')


@app.route('/login')
def login():
    if 'twitter_token' in session:
        del session['twitter_token']
    return twitter.authorize(callback='/oauth-authorized')
#                                next=request.args.get('next') or request.referrer or None))

@app.route('/oauth-authorized')
def oauth_authorized():
    resp = twitter.authorized_response()
    if resp is None:
        flash(u'You denied the request to sign in.')
        return redirect('/sad')
    token = (
        resp['oauth_token'],
        resp['oauth_token_secret']
    )
    user = resp['screen_name']
    session['twitter_token'] = token
    session['twitter_user'] = user
    new_creds = {"token_key": token[0],
                 "token_secret": token[1]}

    utils.delete_credentials(user=user)
    utils.add_credentials(user=user,
                          token_key=token[0],
                          token_secret=token[1],
                          consumer_key=config.CONSUMER_KEY,
                          consumer_secret=config.CONSUMER_SECRET)
    flash('You were signed in as %s' % resp['screen_name'])
    return redirect('/thanks')

@app.route('/thanks')
def thanks():
    return render_template("thanks.html")

@app.route('/sad')
def sad():
    return render_template("sad.html")

@app.route('/')
def index():
#    return 'Please <a href="./login">LOG IN</a> to help with my research :)'
    return render_template('home.html')

@app.route('/hall')
def hall():
    names = [c['user'] for c in utils.get_credentials()] 
    return render_template('thanks.html', names=names)

@app.route('/limits')
def limits():
    creds = utils.get_credentials()
    limits = {}
    for c in creds:
        auth = TOAuth(c['token_key'],
                      c['token_secret'],
                      c['consumer_key'],
                      c['consumer_secret'])
        t = Twitter(auth=auth)
        limits[c["user"]] = json.dumps(t.application.rate_limit_status(), indent=2)

    return render_template('limits.html', limits=limits)

app.secret_key = os.environ.get('SESSION_KEY', 'bitter is cool!')

if __name__ == '__main__':
    app.run()
