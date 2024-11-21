# Makefile for managing submodule updates

SUBMODULE_PATH = data-expert

# Command to update the submodule
update-submodule:
	@echo "Updating submodule..."
	git submodule update --remote $(SUBMODULE_PATH)
	@echo "Submodule updated successfully."

setup:
	@echo "Installing necessary packages for this repository..."
	brew bundle

pre-commit:
	@echo "Setting up pre-commit hooks..."
	pre-commit install --hook-type pre-commit --hook-type pre-push

.PHONY: update-submodule
