// Dynamically connect to the backend WebSocket server
function getDefaultEndpoint() {
    if (window.location.host && window.location.protocol.startsWith("http")) {
        return window.location.host;
    }
    return "10.1.75.51:4214";
}

// ============================================================
// VARIABLES
// ============================================================

let socket = null;
let username = "";
let activeEndpoint = getDefaultEndpoint();


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

const serverInput =
    document.getElementById(
        "server-input"
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


// Pre-fill server endpoint placeholder with default
if (serverInput) {
    serverInput.placeholder = `Endpoint (default: ${getDefaultEndpoint()})`;
}


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

            joinChat();

        }

    }
);

if (serverInput) {
    serverInput.addEventListener(
        "keydown",
        function(event) {

            if (event.key === "Enter") {

                joinChat();

            }

        }
    );
}


// ============================================================
// JOIN CHAT
// ============================================================

function joinChat() {

    username =
        usernameInput.value.trim();


    if (!username) {

        const randomNum =
            Math.floor(1000 + Math.random() * 9000);

        username =
            `Guest_${randomNum}`;

        usernameInput.value =
            username;

    }

    const endpoint = (serverInput && serverInput.value.trim()) || getDefaultEndpoint();
    activeEndpoint = endpoint;

    loginError.textContent =
        `Connecting to ${activeEndpoint}...`;


    connectToServer(
        username,
        activeEndpoint
    );
}


// ============================================================
// CONNECT TO SERVER
// ============================================================

function connectToServer(
    username,
    targetEndpoint
) {

    if (socket) {
        try {
            socket.close();
        } catch (e) {}
    }

    const cleanTarget = (targetEndpoint || getDefaultEndpoint()).replace(/^https?:\/\//, "").replace(/^wss?:\/\//, "").replace(/\/+$/, "");
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${cleanTarget}/ws`;

    try {
        socket = new WebSocket(url);
    } catch (e) {
        loginError.textContent = `Invalid endpoint: ${cleanTarget}`;
        return;
    }


    socket.onopen =
        function() {

            console.log(
                "Connected to server:",
                url
            );


            // Send join request

            socket.send(
                JSON.stringify({

                    type:
                        "authenticate",

                    username:
                        username

                })
            );

        };


    socket.onmessage =
        function(event) {

            try {
                const data =
                    JSON.parse(
                        event.data
                    );


                handleServerMessage(data, cleanTarget);
            } catch (e) {
                console.error("Message parse error:", e);
            }

        };


    socket.onclose =
        function() {

            console.log(
                "Disconnected from server"
            );


            updateConnectionStatus(
                false,
                cleanTarget
            );

        };


    socket.onerror =
        function(error) {

            console.error(
                "WebSocket error:",
                error
            );

            loginError.textContent = `Could not connect to ${cleanTarget}`;

        };

}


// ============================================================
// HANDLE SERVER MESSAGE
// ============================================================

function handleServerMessage(data, endpoint) {


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
            true,
            endpoint
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

        messages.innerHTML = "";

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
                    msg.username || msg.sender || msg["client-name"] || msg["client_name"],
                    msg.message || msg.msg,
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
            data.username || data.sender || data["client-name"] || data["client_name"],
            data.message || data.msg,
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
        `${users.length} Online`;


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


            if (user.backend_id) {
                name.textContent = `${user.username} (${user.backend_id.replace("backend-", "node-")})`;
            } else {
                name.textContent = user.username;
            }


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
    connected,
    endpoint
) {

    if (connected) {

        statusDot.classList.remove(
            "disconnected"
        );


        connectionText.textContent =
            endpoint ? `Connected (${endpoint})` : "Connected";

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
