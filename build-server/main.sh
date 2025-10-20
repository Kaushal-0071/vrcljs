#!/bin/bash

export GIT_REPO="$GIT_REPO"


git clone "$GIT_REPO" /home/app/output

exec node script.js