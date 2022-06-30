package io.dingodb.sdk.client;

public interface IBaseDingoMapper {

    MappingConverter getMappingConverter();

    DingoMapper asMapper();
}
