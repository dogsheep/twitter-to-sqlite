# twitter-to-sqlite

[![PyPI](https://img.shields.io/pypi/v/twitter-to-sqlite.svg)](https://pypi.org/project/twitter-to-sqlite/)
[![CircleCI](https://circleci.com/gh/dogsheep/twitter-to-sqlite.svg?style=svg)](https://circleci.com/gh/dogsheep/twitter-to-sqlite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/dogsheep/twitter-to-sqlite/blob/master/LICENSE)

Save data from Twitter to a SQLite database.

## How to install

    $ pip install twitter-to-sqlite

## Authentication

First, you will need to create a Twitter application at https://developer.twitter.com/en/apps

Once you have created your application, navigate to the "Keys and tokens" page and make note of the following:

* Your API key
* Your API secret key
* Your access token
* Your access token secret

You will need to save all four of these values to a JSON file in order to use this tool.

You can create that tool by running the following command and pasting in the values at the prompts:

    $ twitter-to-sqlite auth
    Create an app here: https://developer.twitter.com/en/apps
    Then navigate to 'Keys and tokens' and paste in the following:

    API key: xxx
    API secret key: xxx
    Access token: xxx
    Access token secret: xxx

This will create a file called `auth.json` in your current directory containing the required values.
 
## Retrieving your Twitter followers

The `twitter-to-sqlite followers` command retrieves details of every follower of the specified account. You can use it to retrieve your own followers, or you can pass a screen_name to pull the followers for another account.

The following command pulls your followers and saves them in a SQLite database file called `twitter.db`:

    $ twitter-to-sqlite followers twitter.db auth.json

The `auth.json` path argument is optional - if you omit it the script will look for `auth.json` in your current directory.

To retrieve followers for another account, use:

    $ twitter-to-sqlite followers twitter.db --screen_name=cleopaws

See [Analyzing my Twitter followers with Datasette](https://simonwillison.net/2018/Jan/28/analyzing-my-twitter-followers/) for the original inspiration for this command.

