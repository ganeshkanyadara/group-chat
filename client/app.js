// Dynamically connect to the backend WebSocket server
const SERVER_IP = window.location.hostname || "localhost";
const SERVER_PORT = 4000;



// ============================================================
// VARIABLES
// ============================================================

let socket = null;

let username = "";


// ============================================================
// HTML ELEMENTS
// ============================================================

const loginScreen =
    document.getElementById(
        "login-screen"
    );


const chatScreen =
    document.getElementById(
        "chat-screen"
    );


const usernameInput =
    document.getElementById(
        "username-input"
    );


const accessCodeInput =
    document.getElementById(
        "access-code-input"
    );


const joinButton =
    document.getElementById(
        "join-button"
    );


const loginError =
    document.getElementById(
        "login-error"
    );


const messageInput =
    document.getElementById(
        "message-input"
    );


const sendButton =
    document.getElementById(
        "send-button"
    );


const messages =
    document.getElementById(
        "messages"
    );


const userList =
    document.getElementById(
        "user-list"
    );


const userCount =
    document.getElementById(
        "user-count"
    );


const statusDot =
    document.getElementById(
        "status-dot"
    );


const connectionText =
    document.getElementById(
        "connection-text"
    );


// ============================================================
// LOGIN
// ============================================================

joinButton.addEventListener(
    "click",
    joinChat
);


usernameInput.addEventListener(
    "keydown",
    function(event) {

        if (event.key === "Enter") {

            accessCodeInput.focus();

        }

    }
);


accessCodeInput.addEventListener(
    "keydown",
    function(event) {

        if (event.key === "Enter") {

            joinChat();

        }

    }
);


// ============================================================
// JOIN CHAT
// ============================================================

function joinChat() {

    username =
        usernameInput.value.trim();


    const accessCode =
        accessCodeInput.value.trim();


    if (!username) {

        loginError.textContent =
            "Please enter your username.";

        return;
    }


    if (!accessCode) {

        loginError.textContent =
            "Please enter your access code.";

        return;
    }


    loginError.textContent =
        "Connecting...";


    connectToServer(
        username,
        accessCode
    );
}


// ============================================================
// CONNECT TO SERVER
// ============================================================

function connectToServer(
    username,
    accessCode
) {

    const url =
        `ws://${SERVER_IP}:${SERVER_PORT}`;


    socket =
        new WebSocket(url);


    socket.onopen =
        function() {

            console.log(
                "Connected to server"
            );


            // Send authentication

            socket.send(
                JSON.stringify({

                    type:
                        "authenticate",

                    username:
                        username,

                    access_code:
                        accessCode

                })
            );

        };


    socket.onmessage =
        function(event) {

            const data =
                JSON.parse(
                    event.data
                );


            handleServerMessage(data);

        };


    socket.onclose =
        function() {

            console.log(
                "Disconnected"
            );


            updateConnectionStatus(
                false
            );

        };


    socket.onerror =
        function(error) {

            console.error(
                "WebSocket error:",
                error
            );

        };

}


// ============================================================
// HANDLE SERVER MESSAGE
// ============================================================

function handleServerMessage(data) {


    // --------------------------------------------------------
    // Authentication successful
    // --------------------------------------------------------

    if (
        data.type ===
        "authenticated"
    ) {

        loginScreen.classList.add(
            "hidden"
        );


        chatScreen.classList.remove(
            "hidden"
        );


        updateConnectionStatus(
            true
        );


        messageInput.focus();


        return;
    }


    // --------------------------------------------------------
    // Authentication / server error
    // --------------------------------------------------------

    if (
        data.type ===
        "error"
    ) {

        loginError.textContent =
            data.message;


        if (socket) {

            socket.close();

        }


        return;
    }


    // --------------------------------------------------------
    // Chat history (sent once on join)
    // --------------------------------------------------------

    if (
        data.type ===
        "history"
    ) {

        // Show a top divider before history messages
        const topDivider =
            document.createElement("div");

        topDivider.className =
            "history-divider";

        topDivider.textContent =
            "— start of history —";

        messages.appendChild(topDivider);


        data.messages.forEach(
            function(msg) {

                addMessage(
                    msg.username,
                    msg.message,
                    msg.timestamp,
                    msg.verified,
                    true        // isHistory flag
                );

            }
        );


        const bottomDivider =
            document.createElement("div");

        bottomDivider.className =
            "history-divider";

        bottomDivider.textContent =
            "— new messages below —";

        messages.appendChild(bottomDivider);


        scrollToBottom();


        return;
    }


    // --------------------------------------------------------
    // System message
    // --------------------------------------------------------

    if (
        data.type ===
        "system"
    ) {

        addSystemMessage(
            data.message
        );


        return;
    }


    // --------------------------------------------------------
    // Live chat message
    // --------------------------------------------------------

    if (
        data.type ===
        "message"
    ) {

        addMessage(
            data.username,
            data.message,
            data.timestamp,
            data.verified,
            false           // not history
        );


        return;
    }


    // --------------------------------------------------------
    // Online users
    // --------------------------------------------------------

    if (
        data.type ===
        "user_list"
    ) {

        updateUserList(
            data.users
        );


        return;
    }

}


// ============================================================
// SEND MESSAGE
// ============================================================

sendButton.addEventListener(
    "click",
    sendMessage
);


messageInput.addEventListener(
    "keydown",
    function(event) {

        if (
            event.key ===
            "Enter"
        ) {

            event.preventDefault();

            sendMessage();

        }

    }
);


function sendMessage() {

    const message =
        messageInput.value.trim();


    if (!message) {

        return;
    }


    if (
        !socket ||
        socket.readyState !==
        WebSocket.OPEN
    ) {

        return;
    }


    socket.send(
        JSON.stringify({

            type:
                "message",

            message:
                message

        })
    );


    messageInput.value = "";

    messageInput.focus();

}


// ============================================================
// DISPLAY MESSAGE
// ============================================================

function addMessage(
    messageUsername,
    message,
    timestamp,
    verified,
    isHistory
) {

    const messageElement =
        document.createElement(
            "div"
        );


    const isOwnMessage =
        messageUsername ===
        username;


    let cls = "message";
    if (isOwnMessage) cls += " own-message";
    if (isHistory)    cls += " history-message";

    messageElement.className = cls;


    // -- Username label ----------------------------------------

    const userElement =
        document.createElement(
            "div"
        );


    userElement.className =
        "message-user";


    userElement.textContent =
        isOwnMessage
            ? "You"
            : messageUsername;


    // -- Message bubble ----------------------------------------

    const textElement =
        document.createElement(
            "div"
        );


    textElement.className =
        "message-text";


    // textContent prevents HTML injection

    textElement.textContent =
        message;


    // -- Signature badge ----------------------------------------

    if (verified !== undefined) {

        const badge =
            document.createElement(
                "span"
            );

        badge.className =
            verified
                ? "verified-badge verified"
                : "verified-badge unverified";

        badge.title =
            verified
                ? "Signature verified ✓"
                : "Signature could not be verified ✗";

        badge.textContent =
            verified ? "✓" : "✗";

        textElement.appendChild(badge);

    }


    // -- Timestamp ---------------------------------------------

    const metaRow =
        document.createElement("div");

    metaRow.className = "message-meta";


    if (timestamp) {

        const timeElement =
            document.createElement(
                "span"
            );

        timeElement.className =
            "message-time";

        timeElement.textContent =
            formatTimestamp(timestamp);

        metaRow.appendChild(timeElement);

    }


    // -- Assemble ----------------------------------------------

    messageElement.appendChild(
        userElement
    );

    messageElement.appendChild(
        textElement
    );

    messageElement.appendChild(
        metaRow
    );

    messages.appendChild(
        messageElement
    );


    scrollToBottom();

}


// ============================================================
// FORMAT TIMESTAMP
// ============================================================

function formatTimestamp(iso) {

    if (!iso) return "";

    try {

        const d = new Date(iso);

        return d.toLocaleTimeString(
            [],
            { hour: "2-digit", minute: "2-digit" }
        );

    } catch (e) {

        return iso;

    }

}


// ============================================================
// SYSTEM MESSAGE
// ============================================================

function addSystemMessage(
    message
) {

    const element =
        document.createElement(
            "div"
        );


    element.className =
        "system-message";


    element.textContent =
        message;


    messages.appendChild(
        element
    );


    scrollToBottom();

}


// ============================================================
// UPDATE USER LIST
// ============================================================

function updateUserList(
    users
) {

    userList.innerHTML = "";


    userCount.textContent =
        `${users.length} / 4`;


    users.forEach(
        function(user) {


            const userElement =
                document.createElement(
                    "div"
                );


            userElement.className =
                "user";


            const avatar =
                document.createElement(
                    "div"
                );


            avatar.className =
                "user-avatar";


            avatar.textContent =
                user.username
                    .charAt(0)
                    .toUpperCase();


            const name =
                document.createElement(
                    "div"
                );


            name.className =
                "user-name";


            name.textContent =
                user.username;


            const dot =
                document.createElement(
                    "div"
                );


            dot.className =
                "online-dot";


            userElement.appendChild(
                avatar
            );


            userElement.appendChild(
                name
            );


            userElement.appendChild(
                dot
            );


            userList.appendChild(
                userElement
            );

        }
    );

}


// ============================================================
// CONNECTION STATUS
// ============================================================

function updateConnectionStatus(
    connected
) {

    if (connected) {

        statusDot.classList.remove(
            "disconnected"
        );


        connectionText.textContent =
            "Connected";

    }

    else {

        statusDot.classList.add(
            "disconnected"
        );


        connectionText.textContent =
            "Disconnected";

    }

}


// ============================================================
// SCROLL
// ============================================================

function scrollToBottom() {

    messages.scrollTop =
        messages.scrollHeight;

}
