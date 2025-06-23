use csv::Writer;
use imessage_database::{
    error::table::TableError,
    tables::{
        chat::Chat,
        messages::Message,
        table::{Diagnostic, Table, get_connection},
    },
    util::dirs::default_db_path,
};
use std::fs::File;

static CHAT_NAME_OR_PHONE_NUMBER: &str = "+19178807627";

fn iter_messages() -> Result<(), TableError> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Create SQL statement
    let mut statement = Message::get(&db)?;

    // Execute statement
    let messages = statement
        .query_map([], |row| Ok(Message::from_row(row)))
        .unwrap();

    // Iterate over each row
    for message in messages {
        let mut msg = Message::extract(message)?;

        // Deserialize message body
        msg.generate_text(&db);

        // Emit debug info for each message
        println!("{:#?}", msg)
    }
    Ok(())
}

fn iter_chats() -> Result<(), TableError> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Create SQL statement
    let mut statement = Chat::get(&db)?;

    // Execute statement
    let chats = statement
        .query_map([], |row| Ok(Chat::from_row(row)))
        .unwrap();

    // Iterate over each row
    for chat in chats {
        let chat = Chat::extract(chat)?;
        println!("{:#?}", chat);
    }
    Ok(())
}

fn run_diagnostics() -> Result<(), TableError> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Run diagnostics
    Message::run_diagnostic(&db)?;
    Ok(())
}

fn export_chats_to_csv() -> Result<(), Box<dyn std::error::Error>> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Create CSV writer
    let file = File::create("chats_export.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write CSV header
    wtr.write_record(&["rowid", "chat_identifier", "service_name", "display_name"])?;

    // Create SQL statement
    let mut statement =
        Chat::get(&db).map_err(|e| format!("Failed to get chat statement: {:?}", e))?;

    // Execute statement
    let chats = statement
        .query_map([], |row| Ok(Chat::from_row(row)))
        .unwrap();

    // Iterate over each row and write to CSV
    for chat in chats {
        let chat = Chat::extract(chat).map_err(|e| format!("Failed to extract chat: {:?}", e))?;

        wtr.write_record(&[
            chat.rowid.to_string(),
            chat.chat_identifier,
            chat.service_name.unwrap_or_else(|| "".to_string()),
            chat.display_name.unwrap_or_else(|| "".to_string()),
        ])?;
    }

    wtr.flush()?;
    println!("Chat data exported to chats_export.csv");
    Ok(())
}

fn find_chat_by_identifier_or_name(
    search_term: &str,
) -> Result<Option<i32>, Box<dyn std::error::Error>> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Create SQL statement to search by chat_identifier or display_name
    let mut statement =
        db.prepare("SELECT rowid FROM chat WHERE chat_identifier = ?1 OR display_name = ?1")?;

    // Execute statement
    let mut rows = statement.query_map([search_term], |row| Ok(row.get::<_, i32>("rowid")?))?;

    // Return the first match
    if let Some(row_result) = rows.next() {
        Ok(Some(row_result?))
    } else {
        Ok(None)
    }
}

fn get_messages_by_chat_id(chat_id: i32) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Create SQL statement to get messages for a specific chat_id
    let mut statement = db.prepare("
        SELECT
            *,
            c.chat_id,
            (SELECT COUNT(*) FROM message_attachment_join a WHERE m.ROWID = a.message_id) as num_attachments,
            d.chat_id as deleted_from,
            (SELECT COUNT(*) FROM message m2 WHERE m2.thread_originator_guid = m.guid) as num_replies
        FROM
            message as m
        LEFT JOIN chat_message_join as c ON m.ROWID = c.message_id
        LEFT JOIN chat_recoverable_message_join as d ON m.ROWID = d.message_id
        WHERE c.chat_id = ?1
        ORDER BY
            m.date
    ").or_else(|_| db.prepare("
        SELECT
            *,
            c.chat_id,
            (SELECT COUNT(*) FROM message_attachment_join a WHERE m.ROWID = a.message_id) as num_attachments,
            NULL as deleted_from,
            0 as num_replies
        FROM
            message as m
        LEFT JOIN chat_message_join as c ON m.ROWID = c.message_id
        WHERE c.chat_id = ?1
        ORDER BY
            m.date
    "))?;

    // Execute statement
    let messages = statement
        .query_map([chat_id], |row| Ok(Message::from_row(row)))
        .map_err(|e| format!("Failed to query messages: {:?}", e))?;

    // Collect messages into a vector
    let mut result = Vec::new();
    for message in messages {
        let msg =
            Message::extract(message).map_err(|e| format!("Failed to extract message: {:?}", e))?;
        result.push(msg);
    }

    Ok(result)
}

fn get_messages_by_handle_id(handle_id: i32) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
    // Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    // Create SQL statement to get messages for a specific handle_id
    let mut statement = db.prepare("
        SELECT
            *,
            c.chat_id,
            (SELECT COUNT(*) FROM message_attachment_join a WHERE m.ROWID = a.message_id) as num_attachments,
            d.chat_id as deleted_from,
            (SELECT COUNT(*) FROM message m2 WHERE m2.thread_originator_guid = m.guid) as num_replies
        FROM
            message as m
        LEFT JOIN chat_message_join as c ON m.ROWID = c.message_id
        LEFT JOIN chat_recoverable_message_join as d ON m.ROWID = d.message_id
        WHERE m.handle_id = ?1
        ORDER BY
            m.date
    ").or_else(|_| db.prepare("
        SELECT
            *,
            c.chat_id,
            (SELECT COUNT(*) FROM message_attachment_join a WHERE m.ROWID = a.message_id) as num_attachments,
            NULL as deleted_from,
            0 as num_replies
        FROM
            message as m
        LEFT JOIN chat_message_join as c ON m.ROWID = c.message_id
        WHERE m.handle_id = ?1
        ORDER BY
            m.date
    "))?;

    // Execute statement
    let messages = statement
        .query_map([handle_id], |row| Ok(Message::from_row(row)))
        .map_err(|e| format!("Failed to query messages: {:?}", e))?;

    // Collect messages into a vector
    let mut result = Vec::new();
    for message in messages {
        let msg =
            Message::extract(message).map_err(|e| format!("Failed to extract message: {:?}", e))?;
        result.push(msg);
    }

    Ok(result)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    match run_diagnostics() {
        Ok(()) => println!("Diagnostics completed successfully!"),
        Err(e) => {
            eprintln!("Diagnostics failed: {}", e);
        }
    }

    // // Query messages by handle_id 270
    // let handle_id = 270;
    // match get_messages_by_handle_id(handle_id) {
    //     Ok(messages) => {
    //         println!("Found {} messages from handle_id {}", messages.len(), handle_id);
    //         for message in messages.iter().take(5) {
    //             // Show first 5 messages
    //             println!("Message {} (handle_id: {:?}): {:?}", message.rowid, message.handle_id, message.text);
    //         }
    //     }
    //     Err(e) => eprintln!("Failed to get messages by handle_id: {}", e),
    // }

    // match iter_messages() {
    //     Ok(()) => println!("Message iteration completed successfully!"),
    //     Err(e) => {
    //         eprintln!("Message iteration failed: {}", e);
    //     }
    // }

    // match iter_chats() {
    //     Ok(()) => println!("Chat iteration completed successfully!"),
    //     Err(e) => {
    //         eprintln!("Chat iteration failed: {}", e);
    //     }
    // }

    // // Export chats to CSV
    // match export_chats_to_csv() {
    //     Ok(()) => println!("CSV export completed successfully!"),
    //     Err(e) => {
    //         eprintln!("CSV export failed: {}", e);
    //     }
    // }

    // // Example usage of the search function
    // let search_term = CHAT_NAME_OR_PHONE_NUMBER; // Replace with actual search term
    // match find_chat_by_identifier_or_name(search_term) {
    //     Ok(Some(rowid)) => {
    //         println!("Found chat with rowid: {}", rowid);

    //         // Get all messages for this chat
    //         match get_messages_by_chat_id(rowid) {
    //             Ok(messages) => {
    //                 println!("Found {} messages in chat {}", messages.len(), rowid);
    //                 for message in messages.iter().take(5) {
    //                     // Show first 5 messages
    //                     println!("Message {} (handle_id: {:?}): {:?}", message.rowid, message.handle_id, message.text);
    //                 }
    //             }
    //             Err(e) => eprintln!("Failed to get messages: {}", e),
    //         }
    //     }
    //     Ok(None) => println!("No chat found for: {}", search_term),
    //     Err(e) => eprintln!("Search failed: {}", e),
    // }



    Ok(())
}
