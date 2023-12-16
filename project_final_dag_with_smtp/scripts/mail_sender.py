import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText




def send_email(email_recepients, email_subject, email_body, email_credentials):
    print(email_recepients, email_subject, email_body, email_credentials)

    server = smtplib.SMTP(
        email_credentials["smtp_server"], email_credentials["smtp_port"]
    )
    server.starttls()
    server.login(email_credentials["account"], email_credentials["password"])
    for recipient in email_recepients:
        print(f"Sending email to {recipient}")
        message = MIMEMultipart("alternative")
        message["From"] = email_credentials["account"]
        message["To"] = recipient
        message["Subject"] = email_subject
        message.attach(MIMEText(email_body, "html"))
        server.sendmail(email_credentials["account"], recipient, message.as_string())
    server.quit()


def send_dag_email_on_success(email_recepients, email_credentials,**kwargs):
    ti = kwargs["ti"]
    context_keyname = kwargs["context_keyname"]
    task_name = "pipeline_allsteps"
    context_message = ti.xcom_pull(task_ids=task_name, key=context_keyname)
    email_subject = "DAG Status: SUCCESS on task {task_name}"
    print("[INFO] Context message is:", context_message)
    email_body = f"Your data count retrieved per table name is: {context_message}"
    send_email(email_recepients, email_subject, email_body, email_credentials)

def send_dag_email_on_failure(email_recepients, email_credentials, **kwargs):
    context = kwargs["context"]
    task_name = context['task_instance_key_str']
    email_body = f" [INFO] Task has failed, task_instance_key_str: {task_name}, check your DAG logs for more information."
    email_subject = "DAG Status: FAILED on task {task_name}"
    send_email(email_recepients, email_subject, email_body, email_credentials)
