package cn.xdf.acdc.devops.api.rest.errors;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.NotAuthorizedException;
import cn.xdf.acdc.devops.service.util.UIError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(EntityNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<Map<String, Object>> notFoundHandler(HttpServletRequest req, EntityNotFoundException e) {
        Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.NOT_FOUND, e.getMessage());
        log.error("错误异常>>>>>>>>>>>>>>> map: {}", map, e);
        return new ResponseEntity<>(map, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(EntityExistsException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ResponseEntity<Map<String, Object>> conflictHandler(HttpServletRequest req, EntityExistsException e) {
        Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.CONFLICT, e.getMessage());
        log.error("错误异常>>>>>>>>>>>>>>> map: {}", map, e);
        return new ResponseEntity<>(map, HttpStatus.CONFLICT);
    }

    @ExceptionHandler(ClientErrorException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<Map<String, Object>> badRequestHandler(HttpServletRequest req, ClientErrorException e) {
        Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.BAD_REQUEST, e.getMessage());
        log.error("错误异常>>>>>>>>>>>>>>> map: {}", map, e);
        return new ResponseEntity<>(map, HttpStatus.BAD_REQUEST);
    }


    @ExceptionHandler(NotAuthorizedException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ResponseEntity<Map<String, Object>> forbiddenHandler(HttpServletRequest req, NotAuthorizedException e) {
        Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.FORBIDDEN, e.getMessage());
        log.error("错误异常>>>>>>>>>>>>>>> map: {}", map, e);
        return new ResponseEntity<>(map, HttpStatus.FORBIDDEN);
    }

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> innerExceptionHandler(HttpServletRequest req, Exception e) {
        Map<String, Object> map = UIError.getDefaultStyleMsg(HttpStatus.INTERNAL_SERVER_ERROR, ErrorMsg.E_105);
        log.error("错误异常>>>>>>>>>>>>>>> map: {}", map, e);
        return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
