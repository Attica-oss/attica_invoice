"""Main module"""
import pretty_errors

from app.app import App

pretty_errors.activate()

def main():
    """main function"""
    test = App()
    test.run()

if __name__ == "__main__":
    main()
