package com.order.application.service;

import com.order.domain.dto.response.admin.AdminDashboardResponse;
import reactor.core.publisher.Mono;

public interface AdminService {

    Mono<AdminDashboardResponse> getDashboard();
}