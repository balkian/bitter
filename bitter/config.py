'''
Common configuration for other modules.
It is not elegant, but it works with flask and the oauth decorators.

Using this module allows you to change the config before loading any other module.
E.g.: 

    import bitter.config as c
    c.CREDENTIALS="/tmp/credentials"
    from bitter.webserver import app
    app.run()
'''
CREDENTIALS = '~/.bitter-credentials.json'
CONFIG_FILE = '~/.bitter.yaml'
