"""Main module"""
import pretty_errors

import asyncio

from app.app import App

pretty_errors.activate()

async def main():
    """main function"""
    test = App()
    await test.run()

if __name__ == "__main__":
    asyncio.run(main())
