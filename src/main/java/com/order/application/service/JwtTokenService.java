package com.order.application.service;

import java.util.List;

public interface JwtTokenService {

    String createToken(String username, List<String> roles);
}