package com.maximus.runner.configuration.secret;

import java.util.Optional;

public interface SecretStore {

    Optional<String> load();

    void save(String value);
}
