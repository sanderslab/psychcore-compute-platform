#!/usr/bin/env bash

# First ensure we're running from the root of the repo
if [ -e .git ]; then
    :
else
    echo "Looks like you're executing the script from the wrong folder."
    echo "Please run from the root of the repository. Where .git folder is located."
    echo "E.g. ./vcs/hooks/install-hooks.sh"
    exit 1
fi

if [ -e .git/hooks/pre-commit ]; then
    echo "Looks like there's already a pre-commit hook installed."
    echo "Please rename (e.g. pre-commit.old) and re-run this installation!"
    exit 1
else
    echo "Installing pre-commit hook..."

    if ! cp vcs/hooks/pre-commit .git/hooks/; then
        echo "Failed to copy to .git/hooks"
        exit 1
    fi

    if ! chmod +x vcs/hooks/pre-commit; then
        echo "Failed to make hook executable."
        exit 1
    fi

    if ! diff vcs/hooks/pre-commit .git/hooks/; then
        # different
        echo "Failed to install pre-commit hook"
        # TODO: clean up
        exit 1
    else
        # identity
        echo "Installed!"
    fi
fi

if [ -e .git/hooks/pre-push ]; then
    echo "Looks like there's already a pre-push hook installed."
    echo "Please delete and re-run this installation!"
    exit 1
else
    echo "Installing pre-push hook..."

    # TODO: cleanup and error reporting
    cp vcs/hooks/pre-push .git/hooks/ || exit 1
    chmod +x vcs/hooks/pre-push || exit 1

    if ! diff vcs/hooks/pre-push .git/hooks/; then
        # different
        echo "Failed to install pre-push hook"
        # TODO: clean up
        exit 1
    else
        # identity
        echo "Installed!"
    fi
fi
