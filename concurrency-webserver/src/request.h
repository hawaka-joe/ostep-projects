#ifndef __REQUEST_H__

void request_handle(int fd);
void request_handle_with_first_line(int fd, char *first_line);
int request_parse_uri(char *uri, char *filename, char *cgiargs);
void request_error(int fd, char *cause, char *errnum, char *shortmsg, char *longmsg);

#endif // __REQUEST_H__
