import asyncio
import json
import websockets


# ============================================================
# CONFIGURATION
# ============================================================

HOST = "0.0.0.0"
PORT = 4000

MAX_USERS = 4


# Only these four people are allowed.
# CHANGE THESE TO YOUR ACTUAL GROUP MEMBERS.

AUTHORIZED_USERS = {
    "ganesh": "12341080",
    "venu": "12341110",
    "venkat": "12341070",
    "dheemanth": "12341710"
}


# websocket -> user information
connected_users = {}


# ============================================================
# SEND JSON TO ONE CLIENT
# ============================================================

async def send_json(websocket, data):

    await websocket.send(
        json.dumps(data)
    )


# ============================================================
# BROADCAST TO ALL CONNECTED USERS
# ============================================================

async def broadcast(data):

    if not connected_users:
        return

    message = json.dumps(data)

    await asyncio.gather(
        *(
            websocket.send(message)
            for websocket in connected_users
        ),
        return_exceptions=True
    )


# ============================================================
# SEND ONLINE USER LIST
# ============================================================

async def send_user_list():

    users = [
        {
            "username": user["username"]
        }
        for user in connected_users.values()
    ]

    await broadcast({
        "type": "user_list",
        "users": users
    })


# ============================================================
# HANDLE CLIENT
# ============================================================

async def chat(websocket):

    ip = websocket.remote_address[0]

    username = None

    try:

        # ----------------------------------------------------
        # First message must contain authentication information
        # ----------------------------------------------------

        raw_message = await websocket.recv()

        try:
            data = json.loads(raw_message)

        except json.JSONDecodeError:

            await send_json(websocket, {
                "type": "error",
                "message": "Invalid authentication request."
            })

            await websocket.close()

            return


        if data.get("type") != "authenticate":

            await send_json(websocket, {
                "type": "error",
                "message": "Authentication required."
            })

            await websocket.close()

            return


        username = data.get("username", "").strip()
        access_code = data.get("access_code", "").strip()


        # ----------------------------------------------------
        # Check username
        # ----------------------------------------------------

        if username not in AUTHORIZED_USERS:

            print(
                f"[REJECTED] Unknown user: "
                f"{username} | IP: {ip}"
            )

            await send_json(websocket, {
                "type": "error",
                "message": "You are not authorized to access this chat."
            })

            await websocket.close()

            return


        # ----------------------------------------------------
        # Check access code
        # ----------------------------------------------------

        if AUTHORIZED_USERS[username] != access_code:

            print(
                f"[REJECTED] Invalid access code for "
                f"{username} | IP: {ip}"
            )

            await send_json(websocket, {
                "type": "error",
                "message": "Invalid access code."
            })

            await websocket.close()

            return


        # ----------------------------------------------------
        # Check if user is already connected
        # ----------------------------------------------------

        existing_usernames = {
            user["username"]
            for user in connected_users.values()
        }


        if username in existing_usernames:

            await send_json(websocket, {
                "type": "error",
                "message":
                    f"{username} is already connected."
            })

            await websocket.close()

            return


        # ----------------------------------------------------
        # Maximum 4 simultaneous users
        # ----------------------------------------------------

        if len(connected_users) >= MAX_USERS:

            print(
                f"[REJECTED] Room full | "
                f"IP: {ip}"
            )

            await send_json(websocket, {
                "type": "error",
                "message":
                    "Chat room is full. "
                    "Maximum 4 users are allowed."
            })

            await websocket.close()

            return


        # ----------------------------------------------------
        # Authentication successful
        # ----------------------------------------------------

        connected_users[websocket] = {
            "username": username,
            "ip": ip
        }


        print(
            f"[JOIN] {username} | IP: {ip}"
        )

        print(
            f"Online users: "
            f"{len(connected_users)}/{MAX_USERS}"
        )


        # Tell client authentication succeeded

        await send_json(websocket, {
            "type": "authenticated",
            "username": username
        })


        # Notify everyone

        await broadcast({
            "type": "system",
            "message":
                f"{username} joined the chat"
        })


        # Update online users

        await send_user_list()


        # ----------------------------------------------------
        # Receive messages
        # ----------------------------------------------------

        async for raw_message in websocket:

            try:

                data = json.loads(raw_message)

            except json.JSONDecodeError:

                continue


            # Only process chat messages

            if data.get("type") != "message":
                continue


            message = data.get(
                "message",
                ""
            ).strip()


            # Ignore empty messages

            if not message:
                continue


            # Maximum message length

            message = message[:1000]


            print(
                f"[MESSAGE] "
                f"{username}: "
                f"{message}"
            )


            # Broadcast message

            await broadcast({
                "type": "message",

                "username": username,

                "message": message
            })


    except websockets.exceptions.ConnectionClosed:

        pass


    except Exception as e:

        print(
            f"[ERROR] "
            f"{username}: {e}"
        )


    finally:

        # ----------------------------------------------------
        # User disconnected
        # ----------------------------------------------------

        if websocket in connected_users:

            user = connected_users.pop(websocket)

            username = user["username"]


            print(
                f"[LEAVE] "
                f"{username} | "
                f"IP: {ip}"
            )

            print(
                f"Online users: "
                f"{len(connected_users)}/{MAX_USERS}"
            )


            # Notify remaining users

            await broadcast({
                "type": "system",

                "message":
                    f"{username} left the chat"
            })


            # Update online users

            await send_user_list()


# ============================================================
# SERVER
# ============================================================

async def main():

    print("=" * 55)

    print(
        "             REAL-TIME GROUP CHAT"
    )

    print("=" * 55)

    print()

    print(
        f"WebSocket server listening on "
        f"{HOST}:{PORT}"
    )

    print(
        f"Maximum users: {MAX_USERS}"
    )

    print()

    print("Authorized users:")

    for username in AUTHORIZED_USERS:

        print(
            f"  - {username}"
        )

    print()

    async with websockets.serve(
        chat,
        HOST,
        PORT
    ):

        print(
            "Server is running..."
        )

        print(
            "Waiting for users..."
        )

        print()

        await asyncio.Future()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\nServer stopped."
        )
