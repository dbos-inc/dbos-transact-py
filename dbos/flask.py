from flask import Flask, request
from werkzeug.wrappers import Request, Response


class FlaskMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        request = Request(environ)
        print("bob1")
        response = self.app(environ, start_response)
        print("bob2")
        return response


def setup_flask_middleware(app: Flask):
    app.wsgi_app = FlaskMiddleware(app.wsgi_app)
