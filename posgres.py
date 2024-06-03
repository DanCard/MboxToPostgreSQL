import mailbox
import psycopg2
import email

insert_counter = 0
spam_counter = 0
trash_counter = 0


def import_mbox_to_postgres(mbox_file_path, db_connection_string):

    # Open the MBOX file
    mbox = mailbox.mbox(mbox_file_path)

    # Iterate over each email
    for msg in mbox:
        # Establish database connection
        with psycopg2.connect(db_connection_string) as conn:
            with conn.cursor() as cur:
                labels = msg.get('X-Gmail-Labels', '').split(',')
                # print(labels)
                if "Spam" in labels:
                    global spam_counter
                    spam_counter += 1
                    print(f" s{spam_counter} ", end='')
                    continue
                if "Trash" in labels:
                    global trash_counter
                    trash_counter += 1
                    print(f" t{trash_counter} ", end='')
                    continue

                # Extract relevant information
                subject = msg['Subject']
                sender = msg['From']
                to_recipients = msg.get_all('To', [])
                cc_recipients = msg.get_all('Cc', [])
                bcc_recipients = msg.get_all('Bcc', [])
                msg_date = msg['Date']
                if msg_date:
                    date = email.utils.parsedate_to_datetime(msg_date)
                else:
                    print("\n date none.  Sender: ", sender)
                    print(subject)
                    print(date)
                    body = str(part.get_payload(decode=True))
                    print(body[:128], "\n")
                    continue

                body = ""

                # Handle multipart messages
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            try:
                                body = part.get_payload(decode=True).decode()
                            except UnicodeDecodeError:
                                # print("\n\tUnicodeDecodeError: ", sender)
                                # print(subject[:32], end='\t')
                                body = str(part.get_payload(decode=True))
                                # print(body[:64])
                else:
                    try:
                        body = msg.get_payload(decode=True).decode()
                    except UnicodeDecodeError:
                        # print("\n\tUnicodeDecodeError: ", sender)
                        # print(subject)
                        # print(date)
                        body = str(part.get_payload(decode=True))
                        # print(body[:32])

                # Insert into database
                try:
                    cur.execute(
                        '''
                        INSERT INTO emails (subject, sender, to_recipients, cc_recipients,
                         bcc_recipients, body, date, labels)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ''',
                        (subject, sender, to_recipients, cc_recipients, bcc_recipients, body, date, labels)
                    )
                except ValueError:
                    print("\n\tValueError: from: ", sender)
                    print(labels)
                    print(date)
                    print(subject)
                    print(body[:256], "\n")
                    continue
                except psycopg2.ProgrammingError:
                    print("\n\tpsycopg2.ProgrammingError: from: ", sender)
                    print("cc: ", cc_recipients, "  type: ", type(cc_recipients), "  len ", len(cc_recipients))
                    for cc in cc_recipients:
                        print("cc: ", cc, "  type: ", type(cc))
                    print(labels)
                    print(subject)
                    print(body[:256], "\n")
                    continue
                except psycopg2.errors.InvalidTimeZoneDisplacementValue:
                    print("\n\tpsycopg2.errors.InvalidTimeZoneDisplacementValue: from: ", sender)
                    print(labels)
                    print(date)
                    print(subject)
                    print(body[:256], "\n")
                    continue
                except psycopg2.errors.InFailedSqlTransaction:
                    print("\n\tpsycopg2.errors.InvalidTimeZoneDisplacementValue: from: ", sender)
                    print(labels)
                    print(date)
                    print(subject)
                    print(body[:256], "\n")
                    continue

                global insert_counter
                insert_counter += 1
                print(f" {insert_counter} ", end=' ')


# Get database connection string from the user (modify for your specific setup)
# db_connection_string = "postgresql://dcar:y@localhost:<port>/emaildb"
db_connection_string = "postgresql://dcar:y@localhost/emaildb"

# Get MBOX file path from user
mbox_file_path = "all-gmail.mbox"

# Import the emails
import_mbox_to_postgres(mbox_file_path, db_connection_string)
