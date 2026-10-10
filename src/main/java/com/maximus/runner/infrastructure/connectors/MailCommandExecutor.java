package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.Timestamp;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import jakarta.mail.*;
import jakarta.mail.internet.*;
import jakarta.mail.search.FlagTerm;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public final class MailCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    private final WorkspacePaths paths;
    public MailCommandExecutor(ConnectorConfig config) throws Exception { this.config = config; paths = new WorkspacePaths(config.workspace()); }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        var request = command.getMail();
        return request.getOperation().equals("send") ? send(request, context) : receive(request, context);
    }
    private CommandResult.Builder send(MailRequest request, CommandContext context) throws Exception {
        var profile = config.connection(request.getConnectionId(), "smtp");
        String protocol = profile.flag("ssl", false) ? "smtps" : "smtp";
        Properties properties = properties(profile, protocol, context);
        boolean authenticate = profile.flag("auth", true);
        properties.setProperty("mail." + protocol + ".auth", Boolean.toString(authenticate));
        var session = Session.getInstance(properties);
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(request.getFrom().isBlank() ? profile.required("from") : request.getFrom(), true));
        recipients(message, Message.RecipientType.TO, request.getToList());
        recipients(message, Message.RecipientType.CC, request.getCcList());
        recipients(message, Message.RecipientType.BCC, request.getBccList());
        if (message.getAllRecipients() == null) throw new IllegalArgumentException("Mail requires at least one recipient");
        message.setSubject(request.getSubject(), "UTF-8"); message.setSentDate(new Date());
        MimeMultipart mixed = new MimeMultipart("mixed");
        MimeBodyPart body = new MimeBodyPart();
        if (!request.getHtml().isBlank()) {
            MimeMultipart alternative = new MimeMultipart("alternative");
            MimeBodyPart plain = new MimeBodyPart(); plain.setText(request.getText(), "UTF-8"); alternative.addBodyPart(plain);
            MimeBodyPart html = new MimeBodyPart(); html.setContent(request.getHtml(), "text/html; charset=UTF-8"); alternative.addBodyPart(html);
            body.setContent(alternative);
        } else body.setText(request.getText(), "UTF-8");
        mixed.addBodyPart(body);
        if (request.getAttachmentsCount() > 10) throw new IllegalArgumentException("At most 10 mail attachments are allowed");
        long bytes = request.getTextBytes().size() + request.getHtmlBytes().size();
        int max = profile.number("maxMessageBytes", 5_242_880);
        for (String attachment : request.getAttachmentsList()) {
            Path file = paths.resolve(attachment);
            if (!Files.isRegularFile(file)) throw new IllegalArgumentException("Attachment is not a file");
            bytes += Files.size(file);
            if (bytes > max) throw new IllegalArgumentException("Mail exceeds configured size limit");
            MimeBodyPart part = new MimeBodyPart(); part.attachFile(file.toFile()); mixed.addBodyPart(part);
        }
        if (bytes > max || max <= 0) throw new IllegalArgumentException("Mail exceeds configured size limit");
        message.setContent(mixed); message.saveChanges();
        Transport transport = context.track(session.getTransport(protocol));
        transport.connect(profile.required("host"), profile.number("port", protocol.equals("smtps") ? 465 : 587),
                authenticate ? profile.required("username") : null, authenticate ? profile.required("password") : null);
        context.remainingMillis(); transport.sendMessage(message, message.getAllRecipients());
        return CommandResult.newBuilder().setMail(MailResponse.newBuilder().setSentMessageId(message.getMessageID()));
    }
    private CommandResult.Builder receive(MailRequest request, CommandContext context) throws Exception {
        if (!Set.of("list", "read").contains(request.getOperation())) throw new IllegalArgumentException("Unsupported mail operation");
        var profile = config.connection(request.getConnectionId(), "imap");
        String protocol = profile.flag("ssl", true) ? "imaps" : "imap";
        var properties = properties(profile, protocol, context);
        properties.setProperty("mail." + protocol + ".peek", "true");
        properties.setProperty("mail." + protocol + ".partialfetch", "true");
        Store store = context.track(Session.getInstance(properties).getStore(protocol));
        store.connect(profile.required("host"), profile.number("port", protocol.equals("imaps") ? 993 : 143), profile.required("username"), profile.required("password"));
        Folder folder = store.getFolder(request.getFolder().isBlank() ? "INBOX" : request.getFolder());
        context.track(() -> { if (folder.isOpen()) folder.close(false); });
        folder.open(Folder.READ_ONLY);
        if (!(folder instanceof UIDFolder uidFolder)) throw new IllegalArgumentException("IMAP UID support is required");
        int limit = request.getLimit() == 0 ? 20 : request.getLimit();
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("Mail limit must be between 1 and 100");
        Message[] messages;
        if (request.getOperation().equals("read")) {
            long uid = Long.parseLong(request.getMessageId());
            if (uid <= 0) throw new IllegalArgumentException("Mail message_id must be a positive IMAP UID");
            Message found = uidFolder.getMessageByUID(uid);
            if (found == null) throw new IllegalArgumentException("Mail UID was not found in this folder");
            messages = new Message[]{found};
        } else if (request.getUnreadOnly()) messages = folder.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
        else {
            int count = folder.getMessageCount();
            messages = count == 0 ? new Message[0] : folder.getMessages(Math.max(1, count - limit + 1), count);
        }
        MailResponse.Builder response = MailResponse.newBuilder();
        for (int i = messages.length - 1; i >= 0 && response.getMessagesCount() < limit; i--) {
            context.remainingMillis(); Message message = messages[i];
            MailItem.Builder item = MailItem.newBuilder().setMessageId(Long.toString(uidFolder.getUID(message)))
                    .setSubject(Objects.toString(message.getSubject(), "")).setSeen(message.isSet(Flags.Flag.SEEN));
            if (message.getFrom() != null) for (Address address : message.getFrom()) item.addFrom(address.toString());
            if (message.getRecipients(Message.RecipientType.TO) != null)
                for (Address address : message.getRecipients(Message.RecipientType.TO)) item.addTo(address.toString());
            if (message.getReceivedDate() != null) item.setReceivedAt(Timestamp.newBuilder().setSeconds(message.getReceivedDate().toInstant().getEpochSecond()));
            if (request.getOperation().equals("read")) item.setText(text(message, Math.max(128, context.maxResultBytes() / 2), context, 0));
            response.addMessages(item);
        }
        return CommandResult.newBuilder().setMail(response).putMetadata("imap_uid_validity", Long.toString(uidFolder.getUIDValidity()));
    }
    private static Properties properties(ConnectorConfig.Profile profile, String protocol, CommandContext context) throws Exception {
        Properties props = new Properties(); String prefix = "mail." + protocol + ".";
        String timeout = Long.toString(Math.min(context.remainingMillis(), 30_000));
        for (String key : List.of("connectiontimeout", "timeout", "writetimeout")) props.setProperty(prefix + key, timeout);
        props.setProperty(prefix + "ssl.checkserveridentity", "true");
        boolean startTls = profile.flag("startTls", protocol.equals("smtp"));
        props.setProperty(prefix + "starttls.enable", Boolean.toString(startTls));
        props.setProperty(prefix + "starttls.required", Boolean.toString(startTls));
        return props;
    }
    private static void recipients(MimeMessage message, Message.RecipientType type, List<String> values) throws Exception {
        for (String value : values) message.addRecipient(type, new InternetAddress(value, true));
    }
    private static String text(Part part, int max, CommandContext context, int depth) throws Exception {
        if (depth > 8) throw new IllegalArgumentException("Mail MIME nesting exceeds limit");
        if (Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition())) return "";
        if (part.isMimeType("text/*")) {
            String charset = new ContentType(part.getContentType()).getParameter("charset");
            try (var input = part.getInputStream()) {
                return new String(BoundedStreams.read(input, max, context), Charset.forName(charset == null ? "UTF-8" : charset));
            }
        }
        if (part.isMimeType("multipart/*")) {
            Multipart multipart = (Multipart) part.getContent();
            if (multipart.getCount() > 50) throw new IllegalArgumentException("Mail MIME parts exceed limit");
            StringBuilder output = new StringBuilder();
            for (int i = 0; i < multipart.getCount(); i++) {
                String value = text(multipart.getBodyPart(i), max, context, depth + 1);
                if (output.length() + value.length() > max) throw new IllegalArgumentException("Mail body exceeds limit");
                output.append(value);
            }
            return output.toString();
        }
        return "";
    }
}
