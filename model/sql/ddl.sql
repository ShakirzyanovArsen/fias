create table if not exists public.region
(
  id     serial primary key,
  aoguid uuid unique  not null,
  aoid   uuid unique  not null,
  code   int          not null,
  name   varchar(255) not null,
  type   varchar(10)  not null
);

create table if not exists public.city
(
  id        serial primary key,
  region_id int references region (id) not null,
  aoguid    uuid unique                not null,
  aoid      uuid unique                not null,
  name      varchar(255)               not null
);

create table if not exists public.street
(
  id      serial primary key,
  city_id int references city (id) not null,
  aoguid  uuid unique              not null,
  aoid    uuid unique              not null,
  name    varchar(255)             not null,
  address text                     not null
);

create table if not exists public.house
(
  id          bigserial primary key,
  street_id   int references street (id),
  guid        uuid unique  not null,
  address     text not null,
  postal_code char(6),
  number      varchar(50),
  build_num   varchar(50),
  struc_num   varchar(50)
);

create table if not exists public.flat
(
  id       bigserial primary key,
  house_id bigint references house (id) not null,
  guid     uuid unique                  not null,
  number   varchar(50)                  not null,
  type     varchar(50)
);
CREATE EXTENSION btree_gist;
create index if not exists flat_house_id_idx on public.flat(house_id);
create index if not exists house_street_id_idx on public.house(street_id);
create index if not exists street_street_id_idx on public.street(city_id);
create index if not exists city_region_id_idx on public.city(region_id);
create index if not exists house_address_idx on public.house using gist(address);