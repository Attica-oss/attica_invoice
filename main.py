"""Main module"""
import asyncio
import pretty_errors



from app.app import App

pretty_errors.activate()

async def main():
    """main function"""
    test = App()
    await test.run()

if __name__ == "__main__":
    asyncio.run(main())
